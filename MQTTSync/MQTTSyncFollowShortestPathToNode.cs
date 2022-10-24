using System;
using System.Collections.Generic;
using System.Linq;
using SimioAPI;
using SimioAPI.Extensions;

namespace MQTTSync
{
    public class MQTTSyncFollowShortestPathToNodeDefinition : ITravelSteeringBehaviorDefinition
    {
        #region ITravelSteeringBehaviorDefinition Members

        /// <summary>
        /// The name of the travel steering behavior. May contain any characters.
        /// </summary>
        public string Name
        {
            get { return "MQTT Sync Follow Network Path"; }
        }

        /// <summary>
        /// Description text for the travel steering behavior.
        /// </summary>
        public string Description
        {
            get { return "MQTT Sync steers an entity to follow a specified network path from a starting node to an ending node, staying within the boundaries of a specified path width."; }
        }

        /// <summary>
        /// Guid which uniquely identifies the travel steering behavior.
        /// </summary>
        public Guid UniqueID
        {
            get { return MY_ID; }
        }
        static readonly Guid MY_ID = new Guid("{55bbf7a7-2d40-44af-8433-71b329c03d94}");

        /// <summary>
        /// Defines the property schema for the travel steering behavior.
        /// </summary>
        public void DefineSchema(IPropertyDefinitions propertyDefinitions)
        {
            var networkName = propertyDefinitions.AddNetworkReferenceProperty("NetworkName");
            networkName.DisplayName = "Network Name";
            networkName.Description = "The network to follow. Defaults to the Global network of the Facility window that contains the entity's current location.";
            networkName.Required = false;
            networkName.SetDefaultString(propertyDefinitions, "Global");

            var startingNode = propertyDefinitions.AddNodeInstanceReferenceProperty("StartingNode");
            startingNode.DisplayName = "Starting Node";
            startingNode.Description = "The starting node of the network path to follow.";
            startingNode.Required = true;
            startingNode.SetDefaultString(propertyDefinitions, "");

            var endingNode = propertyDefinitions.AddNodeInstanceReferenceProperty("EndingNode");
            endingNode.DisplayName = "Ending Node";
            endingNode.Description = "The ending node of the network path to follow.";
            endingNode.Required = true;
            endingNode.SetDefaultString(propertyDefinitions, "");

            var pathWidth = propertyDefinitions.AddExpressionProperty("PathWidth", "Candidate.Link.Size.Width") as IExpressionPropertyDefinition;
            pathWidth.DisplayName = "Path Width";
            pathWidth.Description = "Value that controls how distant the entity can get from the drawn path's centerline.";
            pathWidth.Required = true;
            pathWidth.SupportsCandidateReferences = true;
            pathWidth.UnitType = SimioUnitType.Length;

            var updateTimeInterval = propertyDefinitions.AddExpressionProperty("UpdateTimeInterval", "0.1") as IExpressionPropertyDefinition;
            updateTimeInterval.DisplayName = "Update Time Interval";
            updateTimeInterval.Description = "The time interval between updates checking the entity's adherence to the path.";
            updateTimeInterval.UnitType = SimioUnitType.Time;
            (updateTimeInterval.GetDefaultUnit(propertyDefinitions) as ITimeUnit).Time = TimeUnit.Seconds;
            updateTimeInterval.Required = true;

            var avoidCollisions = propertyDefinitions.AddBooleanProperty("AvoidCollisions");
            avoidCollisions.DisplayName = "Avoid Collisions";
            avoidCollisions.Description = "Indicates whether the entity will attempt to avoid collisions with other entities that are following the same path.";
            avoidCollisions.Required = false;

            var mqttElement = propertyDefinitions.AddElementProperty("MQTTElement", MQTTElementDefinition.MY_ID);
            mqttElement.DisplayName = "MQTTElement";
            mqttElement.Description = "MQTTElement Desc.";
            mqttElement.Required = true;
        }

        /// <summary>
        /// Creates a new instance of the travel steering behavior.
        /// </summary>
        public ITravelSteeringBehavior CreateTravelSteeringBehavior(IPropertyReaders properties, ITravelSteeringBehaviorCreationContext executionContext)
        {
            return new MQTTSyncFollowShortestPathToNode(properties, executionContext);
        }

        #endregion
    }

    // =======================================================================================================
    //
    // We are basing the implementation on "A Predictive Collision Avoidance Model for Multi-Agent Simulation"
    // by Ioannis Karamouzas. Springer: https://link.springer.com/chapter/10.1007/978-3-642-10347-6_4
    //
    // =======================================================================================================

    public class SteerInfo
    {
        public object Original;

        public FacilityLocation EntityLocation;
        public FacilityDirection EntityUnitDirection;
        public double EntityVelocity;
        public double EntityDesiredVelocity;
        public FacilityDirection EntityDirectionalVelocity;
        

        public double EntityRadius;
        public double EntityPsychophysicalRadius;

        public double UniformRandomSampleForAngle;
        public double UniformRandomSampleForDirection;
    }

    public class EvasionTargetInfo
    {
        public object Original;

        public FacilityLocation Location;
        public FacilityDirection DirectionalVelocity;

        public double Radius;
    }

    class FollowShortestPathToNodeManager
    {
        readonly ICalendar _calendar;
        readonly IRandom _random;
        readonly double _timeStep;
        readonly IIntelligentObjectRuntimeData _facilityScope;

        public FollowShortestPathToNodeManager(ICalendar calendar, IRandom random, double timeStep, IIntelligentObjectRuntimeData facilityScope)
        {
            _calendar = calendar;
            _random = random;
            _timeStep = timeStep;
            _facilityScope = facilityScope;
        }

        public static FollowShortestPathToNodeManager GetManagerFrom(IExecutionGlobalData globalData, ICalendar calendar, IRandom random, double timeStep, IIntelligentObjectRuntimeData facilityScope)
        {
            var key = DATA_KEY + timeStep.ToString() + facilityScope.HierarchicalDisplayName;
            var manager = globalData.GetData(key) as FollowShortestPathToNodeManager;
            if (manager == null)
            {
                manager = new FollowShortestPathToNodeManager(calendar, random, timeStep, facilityScope);
                globalData.SetData(key, manager);
            }

            return manager;
        }

        const string DATA_KEY = "a6f918dc-f276-49dc-845a-1a58798ac317";

        private void ScheduleNextEvent()
        {
            _calendar.ScheduleEvent(_calendar.TimeNow + _timeStep, null, _ => DoTimeStep());
        }

        readonly HashSet<MQTTSyncFollowShortestPathToNode> _activeSteeringRules = new HashSet<MQTTSyncFollowShortestPathToNode>();
        public void AddSteeringRule(MQTTSyncFollowShortestPathToNode rule)
        {
            _activeSteeringRules.Add(rule);

            if (_activeSteeringRules.Count == 1)
                ScheduleNextEvent();
        }
        public void RemoveSteeringRule(MQTTSyncFollowShortestPathToNode rule)
        {
            _activeSteeringRules.Remove(rule);
        }

        class UpdateInfo
        {
            public UpdateInfo(MQTTSyncFollowShortestPathToNode rule, TravelSteeringMovement movement, bool bFinished, string finishedTrace, FacilityDirection goalDir)
            {
                RuleInstance = rule;
                Movement = movement;
                Finished = bFinished;
                FinishedTrace = finishedTrace;
                GoalDirection = goalDir;
            }
            public MQTTSyncFollowShortestPathToNode RuleInstance { get; }
            public TravelSteeringMovement Movement { get; }
            public bool Finished { get; }
            public string FinishedTrace { get; }
            public FacilityDirection GoalDirection { get; }
        }

        double[] _randomValues = null;
        const int RANDOM_STREAM = 0;
        UpdateInfo[] _updateList;

        void DoTimeStep()
        {
            if (_activeSteeringRules.Count == 0)
                return;

            //
            // Update cached information about the agents to avoid
            //
            UpdateCachedEvasionTargets();

            if (_updateList == null || _updateList.Length < _activeSteeringRules.Count)
                _updateList = new UpdateInfo[_activeSteeringRules.Count];
            Array.Clear(_updateList, 0, _updateList.Length);

            // Compute random values out of the parallel loop (calling GetRandomSample() is not threadsafe)
            var randomValuesLength = _activeSteeringRules.Count * 2;
            if (_randomValues == null || _randomValues.Length < randomValuesLength)
                _randomValues = new double[randomValuesLength];
            for (int i = 0; i < randomValuesLength; i++)
                _randomValues[i] = _random.GetRandomSample(RANDOM_STREAM);

            //
            // Compute all entity updates
            //
            System.Threading.Tasks.Parallel.ForEach(_activeSteeringRules, (rule, _, j) =>
            {
                bool bFinished = false;
                string finishedTrace = null;
                FacilityDirection goalDir = FacilityDirection.Zero;

                var ruleContext = rule.TravelContext;
                var entityData = ruleContext.EntityData;

                var steerInfo = new SteerInfo()
                {
                    Original = entityData,
                    EntityDesiredVelocity = entityData.DesiredVelocity,
                    EntityDirectionalVelocity = entityData.UnitDirection * entityData.Velocity,
                    EntityLocation = entityData.Location,
                    EntityPsychophysicalRadius = MQTTSyncFollowShortestPathToNode.PsychophysicalRadiusOf(entityData),
                    EntityRadius = MQTTSyncFollowShortestPathToNode.RadiusOf(entityData),
                    EntityUnitDirection = entityData.UnitDirection,
                    EntityVelocity = entityData.Velocity,
                    UniformRandomSampleForAngle = _randomValues[j * 2],
                    UniformRandomSampleForDirection = _randomValues[(j * 2) + 1],
                };
                var movement = rule.ComputeSteeringMovement(steerInfo, _timeStep, rule.MQTTElement, out bFinished, out finishedTrace, out goalDir);

                _updateList[j] = new UpdateInfo(rule, movement, bFinished, finishedTrace, goalDir);
            });

            //
            // Update all entity movements
            //
            foreach(var item in _updateList)
            {
                if (item == null)
                    continue;

                if (item.Movement.OrientInTheSameDirection)
                {
                    item.RuleInstance.TravelContext.SetPreferredMovement(item.Movement);
                }
                else
                {
                    item.RuleInstance.TravelContext.SetPreferredMovement(new TravelSteeringMovement() { Direction = item.GoalDirection.ToUnitLength(), Velocity = 1.0 });
                    item.RuleInstance.TravelContext.SetPreferredMovement(item.Movement);
                }

                if (item.Finished)
                {
                    if (item.FinishedTrace != null)
                        item.RuleInstance.TravelContext.ExecutionInformation.TraceInformation(item.FinishedTrace);

                    item.RuleInstance.TravelContext.MovementFinished();

                    _activeSteeringRules.Remove(item.RuleInstance);
                }
            }
            Array.Clear(_updateList, 0, _updateList.Length);

            ScheduleNextEvent();
        }

        const double GRID_SCALE = 10.0;
        FacilityLocation GridLocationOf(FacilityLocation location)
        {
            return new FacilityLocation(
                (int)((location.X - (location.X < 0 ? GRID_SCALE : 0)) / GRID_SCALE),
                0,
                (int)((location.Z - (location.Z < 0 ? GRID_SCALE : 0)) / GRID_SCALE));
        }

        readonly Dictionary<FacilityLocation, List<EvasionTargetInfo>> _gridLocationToEvasionTargetInfoMap = new Dictionary<FacilityLocation, List<EvasionTargetInfo>>();
        private void UpdateCachedEvasionTargets()
        {
            // Try to re-use lists so we take pressure off the GC
            Stack<List<EvasionTargetInfo>> lists = new Stack<List<EvasionTargetInfo>>();
            foreach (var v in _gridLocationToEvasionTargetInfoMap.Values)
            {
                v.Clear();
                lists.Push(v);
            }

            _gridLocationToEvasionTargetInfoMap.Clear();
            foreach (var agent in _facilityScope.AgentsInFreespace)
            {
                var gridLocation = GridLocationOf(agent.Location);
                List<EvasionTargetInfo> list = null;
                if (_gridLocationToEvasionTargetInfoMap.TryGetValue(gridLocation, out list) == false)
                {
                    if (lists.Count > 0)
                        list = lists.Pop();
                    else
                        list = new List<EvasionTargetInfo>();
                    _gridLocationToEvasionTargetInfoMap[gridLocation] = list;
                }

                var evasionTargetInfo = new EvasionTargetInfo()
                {
                    Original = agent,
                    Location = agent.Location,
                    DirectionalVelocity = agent.UnitDirection * agent.Velocity,
                    Radius = MQTTSyncFollowShortestPathToNode.RadiusOf(agent)
                };

                list.Add(evasionTargetInfo);
            }
        }
        public void GetClosestAgentsTo(SteerInfo info, double radius, List<EvasionTargetInfo> evasionTargets)
        {
            var topLeft = GridLocationOf(info.EntityLocation + new FacilityDirection(-radius, 0, -radius));
            var bottomRight = GridLocationOf(info.EntityLocation + new FacilityDirection(radius, 0, radius));

            for (double x = topLeft.X; x <= bottomRight.X; x++)
            {
                for (double z = topLeft.Z; z <= bottomRight.Z; z++)
                {
                    List<EvasionTargetInfo> evasionTargetInfos = null;
                    if (_gridLocationToEvasionTargetInfoMap.TryGetValue(new FacilityLocation(x, 0, z), out evasionTargetInfos))
                    {
                        foreach (var evTarget in evasionTargetInfos)
                        {
                            // Return everything but the entity making the request
                            if (evTarget.Original != info.Original)
                                evasionTargets.Add(evTarget);
                        }
                    }
                }
            }
        }
    }

    class MQTTSyncFollowShortestPathToNode : ITravelSteeringBehavior
    {
        readonly INodeRuntimeData _startingNode;
        readonly INodeRuntimeData _endingNode;
        readonly INetworkRuntimeData _networkData;
        readonly IExpressionPropertyReader _pathWidth;
        readonly IExpressionPropertyReader _updateTimeInterval;
        readonly bool _avoidCollisions;
        readonly IElementProperty _mqttElementProp;

        readonly BaseSteeringBehaviors.FollowPathInfo _info;

        readonly double _lastLinkWidth;

        readonly FollowShortestPathToNodeManager _agentState;
        readonly IAgentRuntimeData _agentData;
        public MQTTElement MQTTElement;

        public MQTTSyncFollowShortestPathToNode(IPropertyReaders properties, ITravelSteeringBehaviorCreationContext context)
        {
            _agentData = context.EntityData;

            var networkNameProp = properties.GetProperty("NetworkName");
            _networkData = (networkNameProp as IElementReferencePropertyReader).GetElementReference(context) as INetworkRuntimeData;

            if (_networkData == null)
                context.ExecutionInformation.ReportError(networkNameProp, String.Format("Error performing free space travel movement of entity '{0}'. The value for property '{1}' is a null or undefined reference.",
                    _agentData.HierarchicalDisplayName, networkNameProp.Name));

            if (_networkData.HierarchicalDisplayName == "Global" || _networkData.HierarchicalDisplayName.EndsWith(".Global"))
            {
                // Make sure that a 'Global' network reference is the Global network of the Facility window that contains the entity's current location. 
                _networkData = _agentData.CurrentFacility.Global;
            }

            var startingNodeProp = properties.GetProperty("StartingNode");
            _startingNode = (startingNodeProp as IIntelligentObjectReferencePropertyReader).GetIntelligentObjectReference(context) as INodeRuntimeData;

            if (_startingNode == null)
                context.ExecutionInformation.ReportError(startingNodeProp, String.Format("Error performing free space travel movement of entity '{0}'. The value for property '{1}' is a null or undefined reference.",
                    _agentData.HierarchicalDisplayName, startingNodeProp.Name));

            var endingNodeProp = properties.GetProperty("EndingNode");
            _endingNode = (endingNodeProp as IIntelligentObjectReferencePropertyReader).GetIntelligentObjectReference(context) as INodeRuntimeData;

            if (_endingNode == null)
                context.ExecutionInformation.ReportError(endingNodeProp, String.Format("Error performing free space travel movement of entity '{0}'. The value for property '{1}' is a null or undefined reference.",
                    _agentData.HierarchicalDisplayName, endingNodeProp.Name));

            _pathWidth = properties.GetProperty("PathWidth") as IExpressionPropertyReader;
            var updateTimeIntervalPropReader = properties.GetProperty("UpdateTimeInterval");
            _updateTimeInterval = updateTimeIntervalPropReader as IExpressionPropertyReader;

            var timeInterval = 0.0;
            if (MQTTSyncHelper.TryGetPositiveDoubleValue(_updateTimeInterval, context, out timeInterval) == false)
            {
                context.ExecutionInformation.ReportError(updateTimeIntervalPropReader, String.Format("Error performing free space travel movement of entity '{0}'. Time interval is not a positive real value.",
                    _agentData.HierarchicalDisplayName));
                return;
            }

            if (double.IsInfinity(_agentData.Velocity))
            {
                timeInterval = 0.0;
            }

            _mqttElementProp = (IElementProperty)properties.GetProperty("MQTTElement");
            MQTTElement = (MQTTElement)_mqttElementProp.GetElement(context);

            var entityData = context.EntityData as IEntityRuntimeData;
            IEnumerable<IShortestPathInformation> shortestPath = null;

            try
            {
                shortestPath = _startingNode.GetShortestPathTo(_endingNode, _networkData);
            }
            catch(InvalidOperationException)
            {
                // Nodes not on the network
                shortestPath = null;
            }

            if (shortestPath == null)
                context.ExecutionInformation.ReportError(String.Format("Error performing free space travel movement of entity '{0}'. No shortest path can be found between '{1}' and '{2}' using network '{3}'.",
                    _agentData.HierarchicalDisplayName,
                    _startingNode.HierarchicalDisplayName,
                    _endingNode.HierarchicalDisplayName,
                    _networkData.HierarchicalDisplayName));

            _info = new BaseSteeringBehaviors.FollowPathInfo(VertexInfosFrom(_startingNode, shortestPath, context, _pathWidth));

            if (shortestPath.Any())
                _lastLinkWidth = NormalizedLinkWidth(shortestPath.Last().Link, context, _pathWidth);
            else
                _lastLinkWidth = 1.0;

            var avoidCollisionsProp = properties.GetProperty("AvoidCollisions");
            _avoidCollisions = avoidCollisionsProp.GetDoubleValue(context) == 1.0;

            context.ExecutionInformation.TraceInformation(String.Format("Entity '{0}' starting free space travel movement following the shortest path from '{1}' to '{2}' using network '{3}'.",
                context.EntityData.HierarchicalDisplayName,
                _startingNode.HierarchicalDisplayName, 
                _endingNode.HierarchicalDisplayName, 
                _networkData.HierarchicalDisplayName));

            _agentState = FollowShortestPathToNodeManager.GetManagerFrom(context.ModelGlobalData, context.Calendar, context.Random, timeInterval, context.EntityData.CurrentFacility);
            _agentState.AddSteeringRule(this);
        }

        static IEnumerable<FacilityLocation> LinkInteriorVerticesFrom(IShortestPathInformation info)
        {
            if (info.Link.End == info.EndingNode)
            {
                // Forward on the link
                foreach (var p in info.Link.InteriorVertices)
                    yield return p;
            }
            else
            {
                // Backward on the link
                foreach (var p in info.Link.InteriorVertices.Reverse())
                    yield return p;
            }
        }
        static double NormalizedLinkWidth(ILinkRuntimeData link, IExecutionContext context, IExpressionPropertyReader pathWidthProperty)
        {
            var candidateContext = context.WithCandidate(link);
            var widthValue = pathWidthProperty.GetExpressionValue(candidateContext);
            if (widthValue is double)
            {
                var widthDoubleValue = (double)widthValue;
                if (widthDoubleValue >= 0 && Double.IsNaN(widthDoubleValue) == false && Double.IsInfinity(widthDoubleValue) == false)
                    return widthDoubleValue;
            }

            // Something went wrong
            context.ExecutionInformation.ReportError(pathWidthProperty as IPropertyReader, String.Format("The path width expression did not return a valid real number for the width of link '{0}'", link.HierarchicalDisplayName));

            return 1.0; // Default to 1 meter
        }
        static IEnumerable<BaseSteeringBehaviors.FollowPathVertexInfo> VertexInfosFrom(INodeRuntimeData startingNode, IEnumerable<IShortestPathInformation> shortestPath, IExecutionContext context, IExpressionPropertyReader pathWidthProperty)
        {
            if (shortestPath.Any())
            {
                yield return new BaseSteeringBehaviors.FollowPathVertexInfo(startingNode.Location, NormalizedLinkWidth(shortestPath.First().Link, context, pathWidthProperty));

                bool bFirst = true;
                FacilityLocation lastEndPoint = new FacilityLocation();

                foreach (var spi in shortestPath)
                {
                    var linkWidth = NormalizedLinkWidth(spi.Link, context, pathWidthProperty);

                    if (!bFirst)
                        yield return new BaseSteeringBehaviors.FollowPathVertexInfo(lastEndPoint, linkWidth);

                    foreach (var v in LinkInteriorVerticesFrom(spi))
                    {
                        yield return new BaseSteeringBehaviors.FollowPathVertexInfo(v, linkWidth);
                    }

                    yield return new BaseSteeringBehaviors.FollowPathVertexInfo(spi.EndingNode.Location, linkWidth);
                    lastEndPoint = spi.EndingNode.Location;
                    bFirst = false;
                }
            }
            else
            {
                // Return something here, though it will never really get used, we *should* just immediately end the travel, because
                // this case is when startingNode == endingNode
                yield return new BaseSteeringBehaviors.FollowPathVertexInfo(startingNode.Location, 1.0);
            }
        }

        #region ITravelSteeringBehavior Members

        const double METERS_PER_HOURSQ_PER_METERS_PER_SECONDSQ = 12960000.0; // 12,960,000 m/h^2 == 1 m/s^2

        //
        // These are the tweakable values for the rule
        //
        const double GOAL_FORCE_VELOCITY_CHANGE_TIME = 0.4 / 3600.0; // 0.4 seconds
        const double ANTICIPATION_TIME = 8.0 / 3600.0; // 8 seconds
        const int MAX_NEARBY_AGENTS_TO_EVADE = 5;
        const double NOISE_DISTANCE_SCALE = 0.25;
        const double PSYCHOPHYSICAL_RADIUS_SCALE = 0.25;
        const double MIN_DISTANCE_TO_TRAVEL_TO_CHANGE_DIRECTION = 0.025;

        public ITravelSteeringBehaviorContext TravelContext => _context;
        ITravelSteeringBehaviorContext _context;
        public void Steer(ITravelSteeringBehaviorContext context)
        {
            _context = context;
            _agentState.AddSteeringRule(this);
        }

        public static double RadiusOf(IAgentRuntimeData agentData)
        {
            return Math.Sqrt(agentData.Size.Length * agentData.Size.Length + agentData.Size.Width * agentData.Size.Width) / 2.0;
        }

        public static double PsychophysicalRadiusOf(IAgentRuntimeData agentData)
        {
            // How far away from other agents it needs to be to feel comfortable.
            return RadiusOf(agentData) * PSYCHOPHYSICAL_RADIUS_SCALE;
        }

        public TravelSteeringMovement ComputeSteeringMovement(SteerInfo steerInfo, double timeInterval, MQTTElement mqttElement, out bool bFinished, out string finishedTrace, out FacilityDirection goalDir)
        {
            //
            // See if we have reached the destination. If we are at least half the width of the link away from the last node, we are done.
            //  Alternatively, if we might overshoot it, we are also done.
            //
            double distanceFromLastNode = 0.0;
            //var overrideFacilityLocation = new FacilityLocation(mqttElement.getXValue(), mqttElement.getYValue(), mqttElement.getZValue());
            //if (mqttElement.getUseXYZ() == true)
            //{                
            //    distanceFromLastNode = (overrideFacilityLocation - steerInfo.EntityLocation).ComputeLength();                
            //}
            //else
            //{
                distanceFromLastNode = (_endingNode.Location - steerInfo.EntityLocation).ComputeLength();
            //}
            if (double.IsInfinity(_agentData.Velocity) || mqttElement.getUpdate() || distanceFromLastNode < _lastLinkWidth / 2.0 || distanceFromLastNode < (steerInfo.EntityVelocity * timeInterval))
            {
                //
                // We set the movement one more time to make sure the entity is now not trying to rise off the floor
                //
                var zeroYUnitDirection = new FacilityDirection(steerInfo.EntityUnitDirection.dX, 0.0, steerInfo.EntityUnitDirection.dZ);

                if (mqttElement.getUpdate() == true)
                {
                    mqttElement.setUpdate(false);                  
                }
                //else
                //{
                    //
                    // This steering rule is done
                    //
                    bFinished = true;
                    finishedTrace = String.Format("Entity '{0}' finished free space travel movement following the shortest path from '{1}' to '{2}' using network '{3}'.",
                        _context.EntityData.HierarchicalDisplayName,
                        _startingNode.HierarchicalDisplayName,
                        _endingNode.HierarchicalDisplayName,
                        _networkData.HierarchicalDisplayName);
                    goalDir = FacilityDirection.Zero;
                    //_context.MovementFinished();

                    //
                    // We are done with this agent
                    //
                    return new TravelSteeringMovement()
                    {
                        Direction = zeroYUnitDirection,
                        Velocity = _context.EntityData.Velocity,
                        OrientInTheSameDirection = true,
                        Acceleration = 0,
                        AccelerationDuration = 0,
                        AccelerationDirection = FacilityDirection.Zero
                    };
                //}
            }

            FacilityDirection forceGoal;
            // if override
            //if (mqttElement.getUseXYZ() == true)
            //{
            //    forceGoal = ComputeGoalForceOverrideFacilityLocation(steerInfo, overrideFacilityLocation, out goalDir);
            //}
            //else
            //{
                forceGoal = ComputeGoalForce(steerInfo, timeInterval, out goalDir);
            //}
            var forceWall = ComputeWallForce(steerInfo);
            var forceEvasion = ComputeEvasionForce(steerInfo, forceGoal, forceWall, timeInterval);
            var forceNoise = ComputeNoiseForce(steerInfo);

            var force = forceGoal + forceWall + forceEvasion + forceNoise;

            var forceNoY = new FacilityDirection(force.dX, 0.0, force.dZ);

            bFinished = false;
            finishedTrace = null;

            var expectedDistance = (forceNoY * (timeInterval * timeInterval) * 0.5 + (_context.EntityData.UnitDirection * _context.EntityData.Velocity) * timeInterval).ComputeLength();

            return new TravelSteeringMovement()
            {
                Direction = _context.EntityData.UnitDirection,
                Velocity = _context.EntityData.Velocity,
                OrientInTheSameDirection = expectedDistance > MIN_DISTANCE_TO_TRAVEL_TO_CHANGE_DIRECTION,
                Acceleration = forceNoY.ComputeLength(),
                AccelerationDuration = timeInterval, // TIME_TO_CHANGE_DIRECTION
                AccelerationDirection = forceNoY
            };

        }

        private FacilityDirection ComputeGoalForce(SteerInfo steerInfo, double timeInterval, out FacilityDirection goalDir)
        {
            //
            // We are doing a very simple goal/path finding here of simply trying to stay on the path, and if we are already 
            //  on the path, continue to move in the direction that segment of the path is going
            //

            var entityDirectionalVelocity = steerInfo.EntityDirectionalVelocity;

            bool bOnPath = false;
            goalDir = BaseSteeringBehaviors.FollowPathGoalDirection(_info, timeInterval,
                steerInfo.EntityVelocity, steerInfo.EntityLocation, steerInfo.EntityUnitDirection, out bOnPath);

            var desiredVelocity = steerInfo.EntityDesiredVelocity;

            var forceGoal = (goalDir * desiredVelocity - steerInfo.EntityDirectionalVelocity) / GOAL_FORCE_VELOCITY_CHANGE_TIME;
            return forceGoal;
        }
        private FacilityDirection ComputeGoalForceOverrideFacilityLocation(SteerInfo steerInfo, FacilityLocation overrideFacilityLocation, out FacilityDirection goalDir)
        {
            //
            // We are doing a very simple goal/path finding here of simply trying to stay on the path, and if we are already 
            //  on the path, continue to move in the direction that segment of the path is going
            //

            var entityDirectionalVelocity = steerInfo.EntityDirectionalVelocity;

            goalDir = BaseSteeringBehaviors.SteerForSeekWithoutChangeInSpeed(overrideFacilityLocation, steerInfo.EntityLocation, steerInfo.EntityUnitDirection);

            var desiredVelocity = steerInfo.EntityDesiredVelocity;

            var forceGoal = (goalDir * desiredVelocity - steerInfo.EntityDirectionalVelocity) / GOAL_FORCE_VELOCITY_CHANGE_TIME;
            return forceGoal;
        }
        private FacilityDirection ComputeWallForce(SteerInfo steerInfo)
        {
            //
            // For now, we don't support walls, so... nothing
            //
            return FacilityDirection.Zero;
        }

        struct EvasionAgentInfo
        {
            public EvasionAgentInfo(EvasionTargetInfo agentData, double intersectionTime)
            {
                AgentData = agentData;
                IntersectionTime = intersectionTime;
            }
            public readonly EvasionTargetInfo AgentData;
            public readonly double IntersectionTime;
        };

        static readonly Comparison<EvasionAgentInfo> _intersectionTimeComparison;
        static MQTTSyncFollowShortestPathToNode()
        {
            _intersectionTimeComparison = new Comparison<EvasionAgentInfo>((a, b) => a.IntersectionTime.CompareTo(b.IntersectionTime));
        }

        readonly List<EvasionTargetInfo> _closeAgents = new List<EvasionTargetInfo>(10);
        readonly List<EvasionAgentInfo> _agentsOnCollisionCourse = new List<EvasionAgentInfo>(10);
        private FacilityDirection ComputeEvasionForce(SteerInfo context, FacilityDirection forceGoal, FacilityDirection forceWall, double timeStep)
        {
            if (_avoidCollisions == false)
                return FacilityDirection.Zero;

            var entityLocation = context.EntityLocation;

            var entityRadius = context.EntityRadius;
            var entityPsyRadius = context.EntityPsychophysicalRadius;

            var vDes = (context.EntityDirectionalVelocity) + ((forceGoal + forceWall) * timeStep);

            _closeAgents.Clear();
            _agentState.GetClosestAgentsTo(context, context.EntityVelocity * Math.Max(timeStep, ANTICIPATION_TIME) + context.EntityRadius, _closeAgents);

            _agentsOnCollisionCourse.Clear();
            foreach(var a in _closeAgents)
            {
                var aLocation = a.Location;

                // Check if the other agent is "in front" of the entity (the entity can "see" it to move away from it)
                var unitDirToAgent = (aLocation - entityLocation).ToUnitLength();
                if (unitDirToAgent.Dot(context.EntityUnitDirection) <= 0)
                    continue;

                var v = vDes - a.DirectionalVelocity;
                var intersectionTime = RayCircleIntersectionTime(v, entityLocation, aLocation, a.Radius + entityPsyRadius);
                if (intersectionTime >= 0 && intersectionTime < ANTICIPATION_TIME)
                    _agentsOnCollisionCourse.Add(new EvasionAgentInfo(a, intersectionTime));
            }

            _agentsOnCollisionCourse.Sort(_intersectionTimeComparison);

            // Max acceleration is to go completely reverse full speed in the allotted time
            var maxEvasionMagnitude = (context.EntityDesiredVelocity * 2.0) / GOAL_FORCE_VELOCITY_CHANGE_TIME;

            FacilityDirection lastFv = FacilityDirection.Zero;
            for (int j = 0; j < _agentsOnCollisionCourse.Count && j < MAX_NEARBY_AGENTS_TO_EVADE; j++)
            {
                var bIntersection = false;                
                var Fv = ComputeIterativeEvasionForce(_agentsOnCollisionCourse, j + 1, context, 
                    maxEvasionMagnitude,
                    forceGoal, forceWall, lastFv, timeStep, out bIntersection);

                // No intersections, we are done
                if (!bIntersection)
                    break;

                lastFv = Fv;              
            }

            // The paper says "Then, the total evasive force Fe of Ai is determined as the average of all the forces Fv."
            //  you would think you would just return the *last* Fv computed however, since it (might) have avoided all 
            //  collisions, so we'll do that
            return lastFv;
        }

        private static FacilityDirection ComputeIterativeEvasionForce(List<EvasionAgentInfo> agentInfos, int endIndex, SteerInfo steerInfo, double maxEvasionMagnitude,
            FacilityDirection forceGoal, FacilityDirection forceWall, FacilityDirection forcePreviousEvade, double timeStep, out bool bIntersection)
        {
            bIntersection = false;

            var vDes = (steerInfo.EntityDirectionalVelocity) + ((forceGoal + forceWall + forcePreviousEvade) * timeStep);

            FacilityDirection evasionForce = FacilityDirection.Zero;

            for (int j = 0; j < agentInfos.Count && j <= endIndex; j++)
            {
                var agentOnCollisionCourse = agentInfos[j];

                var a = agentOnCollisionCourse.AgentData;
                var aLocation = a.Location;
                var aRadius = a.Radius;

                var v = vDes - a.DirectionalVelocity;
                var intersectionTime = RayCircleIntersectionTime(v, steerInfo.EntityLocation, aLocation, aRadius + steerInfo.EntityPsychophysicalRadius);
                if (intersectionTime >= 0 && intersectionTime < ANTICIPATION_TIME)
                {
                    bIntersection = true;

                    var tcij = agentOnCollisionCourse.IntersectionTime;

                    var ci = steerInfo.EntityLocation + vDes * tcij;
                    var cj = aLocation + a.DirectionalVelocity * tcij;

                    var ncicjDir = ci - cj;
                    var ncicj = FacilityDirection.Zero;
                    var ncicjDirLen = ncicjDir.ComputeLength();
                    if (ncicjDirLen != 0.0)
                        ncicj = ncicjDir / ncicjDirLen;

                    var D = Math.Max(Double.Epsilon, (ci - steerInfo.EntityLocation).ComputeLength() + Math.Max(0.0, ((ci - cj).ComputeLength() - steerInfo.EntityRadius - aRadius)));

                    // The paper had example values in relation to m/s^2 but we are in m/h^2
                    var potentialMagnitude = GetMagnitudeOfEvasiveForce(D) * METERS_PER_HOURSQ_PER_METERS_PER_SECONDSQ;

                    var Fij = ncicj * Math.Min(maxEvasionMagnitude, potentialMagnitude);

                    evasionForce += Fij;
                }
            }

            return evasionForce;
        }
     
        private static double GetMagnitudeOfEvasiveForce(double D)
        {
            var dMin = 1.0;
            var dMid = 8.0;
            var dMax = 10.0;
            var alpha = 1.0;
            var beta = 3.0;

            if ((0 < D) && (D < dMin))
                return (alpha / D) + beta;
            else if ((dMin <= D) && (D < dMid))
                return beta;
            else if ((dMid <= D) && (D < dMax))
                return beta * (dMax - D) / (dMax - dMid);
            else if (dMax <= D)
                return 0;

            return 0;
        }

        private static FacilityDirection ComputeNoiseForce(SteerInfo context)
        {
            var angle = System.Math.PI * 2.0 * context.UniformRandomSampleForAngle;
            var distance = NOISE_DISTANCE_SCALE * (context.EntityDesiredVelocity / GOAL_FORCE_VELOCITY_CHANGE_TIME) * context.UniformRandomSampleForDirection;

            return new FacilityDirection(Math.Cos(angle), 0, Math.Sin(angle)) * distance;
        }

        private static double RayCircleIntersectionTime(FacilityDirection rayDir, FacilityLocation rayStart, FacilityLocation circleCenter, double radius)
        {
            var d = rayDir;
            var f = rayStart - circleCenter;

            var a = d.Dot(d);
            var b = 2 * f.Dot(d);
            var c = f.Dot(f) - radius * radius;

            var discriminant = b * b - 4 * a * c;
            if (discriminant < 0)
            {
                // no intersection
                return Double.MinValue;
            }
            else
            {
                // ray didn't totally miss sphere,
                // so there is a solution to
                // the equation.

                discriminant = Math.Sqrt(discriminant);
              
                // either solution may be on or off the ray so need to test both
                // t1 is always the smaller value, because BOTH discriminant and
                // a are nonnegative.
                var t1 = (-b - discriminant) / (2 * a);
                var t2 = (-b + discriminant) / (2 * a);

                if (t1 <= 0 && t2 <= 0)
                {
                    return Double.MinValue; // Past collision
                }
                else if ((t1 <= 0 && 0 <= t2) || (t2 <= 0 && 0 <= t1))
                {
                    return 0.0;
                }

                return Math.Min(t1, t2);
            }
        }

        public void OnTravelCancelling(IExecutionContext context)
        {
            _agentState.RemoveSteeringRule(this);
        }

        public void OnTravelSuspended()
        {
        }

        public void OnTravelResumed()
        {
        }

        #endregion
    }
}
