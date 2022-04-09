using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using SimioAPI;
using SimioAPI.Extensions;

namespace MQTTSync
{
    static class BaseSteeringBehaviors
    {
        public class WanderInfo
        {
            protected WanderInfo(FacilityDirection steeringDirection)
            {
                SteeringDirection = steeringDirection;
            }

            public readonly FacilityDirection SteeringDirection;
        }

        private class WanderInfoImpl : WanderInfo
        {
            public WanderInfoImpl(FacilityDirection wanderUnitDirection, FacilityDirection steeringDirection)
                : base(steeringDirection)
            {
                LastWanderUnitDirection = wanderUnitDirection;
            }

            public readonly FacilityDirection LastWanderUnitDirection;
        }

        const int RANDOM_STREAM = 0;

        public static double RandomBetween1andNeg1(SimioAPI.IRandom random)
        {
            return (random.GetRandomSample(RANDOM_STREAM) * 2.0) - 1.0;
        }
        public static FacilityDirection RandomUnitDirectionOnFloor(SimioAPI.IRandom random)
        {
            return new FacilityDirection(RandomBetween1andNeg1(random), 0/*RandomBetween1andNeg1(random)*/, RandomBetween1andNeg1(random));
        }

        static readonly FacilityDirection WANDER_BASE_DIRECTION = new FacilityDirection(1, 0, 0);
        static readonly double WANDER_SCALE = 3600.0; // 1 meters per second (default speed of an entity) in meters per hour. This is a kind of magic number so that the wander rate and strength don't have to be huge numbers
        static readonly double WANDER_BIG_CIRCLE_OFFSET = 7200.0; // 2 meters per second (so with a wander strength of one the default speed goes between 2 +/- 1 meters per second (in other words between 1 and 3 meters per second)

        /// <summary>
        /// Wanders without changing the velocity magnitude
        /// </summary>
        public static WanderInfo Wander(WanderInfo existingWanderInfo, FacilityDirection currentVelocity, double wanderStrength, double wanderRate, SimioAPI.IRandom random)
        {
            // This is implemented according to the recommendations of Reynolds for the Wander behavior in the paper here
            // http://www.red3d.com/cwr/papers/1999/gdc99steer.pdf.
            // "This idea can be implemented several ways, but one that has produced good results is to constrain the 
            //   steering force to the surface of a sphere located slightly ahead of the character. To produce the 
            //   steering force for the next frame: a random displacement is added to the previous value, and the sum 
            //   is constrained again to the sphere’s surface. The sphere’s radius determines the maximum wandering 
            //   "strength" and the magnitude of the random displacement determines the wander "rate.""

            var existingWanderInfoImpl = existingWanderInfo as WanderInfoImpl;

            if (existingWanderInfoImpl == null)
                existingWanderInfoImpl = new WanderInfoImpl(RandomUnitDirectionOnFloor(random), new FacilityDirection());

            var startPointOnBigCircle = (existingWanderInfoImpl.LastWanderUnitDirection) * wanderStrength * WANDER_SCALE;
            var wanderOffset = RandomUnitDirectionOnFloor(random) * wanderRate * WANDER_SCALE;
            var nextPoint = startPointOnBigCircle + wanderOffset;

            // We rotate the nextPoint on the circle by the angle between the velocity and the Wander base direction
            var nextPointInVelocitySpace = RotateToMatchAngleOnFloor(nextPoint, currentVelocity, WANDER_BASE_DIRECTION);

            var nextPointOnCircleInVelocitySpace = nextPointInVelocitySpace.ToUnitLength() * wanderStrength * WANDER_SCALE;

            var bigCircleCenterOffsetFromVehicle = currentVelocity.ToUnitLength() * (WANDER_BIG_CIRCLE_OFFSET);

            var relativeVelocityOnCircle = bigCircleCenterOffsetFromVehicle + nextPointOnCircleInVelocitySpace;

            //var steeringDirection = (relativeVelocityOnCircle.ToUnitLength() * currentVelocity.ComputeLength()) - currentVelocity;
            var steeringDirection = (relativeVelocityOnCircle) - currentVelocity;

            return new WanderInfoImpl(nextPoint.ToUnitLength(), steeringDirection);            
        }

        static FacilityDirection RotateToMatchAngleOnFloor(FacilityDirection toRotate, FacilityDirection match, FacilityDirection standardDir)
        {
            var matchUnit = match.ToUnitLength();

            var cosAngle = standardDir.Dot(matchUnit);
            var angle = System.Math.Acos(cosAngle);
            if ((standardDir.dX * matchUnit.dZ - standardDir.dZ * matchUnit.dX) > 0)
                angle = -angle;

            cosAngle = System.Math.Cos(angle);
            var sinAngle = System.Math.Sin(angle);

            var nextPointUnit = toRotate.ToUnitLength();

            var nextPointInVelocitySpaceX = nextPointUnit.dX * cosAngle + nextPointUnit.dZ * sinAngle;
            var nextPointInVelocitySpaceZ = nextPointUnit.dZ * cosAngle - nextPointUnit.dX * sinAngle;

            return new FacilityDirection(nextPointInVelocitySpaceX, 0, nextPointInVelocitySpaceZ);
        }

        public struct FollowPathVertexInfo
        {
            public readonly FacilityLocation Location;
            public readonly double LinkWidth;

            public FollowPathVertexInfo(FacilityLocation location, double linkWidth)
            {
                Location = location;
                LinkWidth = linkWidth;
            }
        }
        public class FollowPathInfo
        {
            readonly FacilityLocation[] _vertices;
            readonly FacilityDirection[] _tangents;
            readonly double[] _lengths;
            readonly double _totalLength;
            readonly double[] _radiuses;

            public FollowPathInfo(IEnumerable<FollowPathVertexInfo> vertices)
            {
                _vertices = vertices.Select(vi => vi.Location).ToArray();
                _radiuses = vertices.Select(vi => vi.LinkWidth / 2.0).ToArray();
                _tangents = new FacilityDirection[_vertices.Length];
                _lengths = new double[_vertices.Length];
                for (int i = 0; i < _vertices.Length - 1; i++)
                {
                    var tangent = _vertices[i + 1] - _vertices[i];
                    _lengths[i] = tangent.ComputeLength();
                    _tangents[i] = tangent.ToUnitLength();
                    _totalLength += _lengths[i];
                }
            }

            internal double MapPointToPathDistance(FacilityLocation point)
            {
                double minDistance = double.MaxValue;
                double segmentLengthTotal = 0;
                double pathDistance = 0;

                for (int i = 0; i < _vertices.Length - 1; i++)
                {
                    FacilityLocation chosen;
                    double segmentProjection;
                    double d = PointToSegmentDistance(point, _vertices[i], _vertices[i + 1], _tangents[i], _lengths[i], out chosen, out segmentProjection);
                    if (d < minDistance)
                    {
                        minDistance = d;
                        pathDistance = segmentLengthTotal + segmentProjection;
                    }
                    segmentLengthTotal += _lengths[i];
                }

                // return distance along path of onPath point
                return pathDistance;
            }
            internal FacilityLocation MapPointToPath(FacilityLocation point, out FacilityDirection tangent, out double outside)
            {
                double minDistance = double.MaxValue;
                FacilityLocation onPath = new FacilityLocation(0, 0, 0);
                tangent = new FacilityDirection(0, 0, 0);

                double radius = 0.0;

                // loop over all segments, find the one nearest to the given point
                for (int i = 0; i < _vertices.Length - 1; i++)
                {
                    FacilityLocation chosen;
                    double segmentProjection;
                    double d = PointToSegmentDistance(point, _vertices[i], _vertices[i + 1], _tangents[i], _lengths[i], out chosen, out segmentProjection);
                    if (d < minDistance)
                    {
                        minDistance = d;
                        onPath = chosen;
                        tangent = _tangents[i];
                        radius = _radiuses[i];
                    }
                }

                // measure how far original point is outside the Pathway's "tube"
                outside = Distance(onPath, point) - radius;

                // return point on path
                return onPath;
            }
            internal FacilityLocation MapPathDistanceToPoint(double pathDistance)
            {
                if (pathDistance < 0)
                    return _vertices[0];
                if (pathDistance >= _totalLength)
                    return _vertices[_vertices.Length - 1];

                double remaining = pathDistance;

                // step through segments, subtracting off segment lengths until
                // locating the segment that contains the original pathDistance.
                // Interpolate along that segment to find 3d point value to return.
                FacilityLocation result = new FacilityLocation(0, 0, 0);
                for (int i = 0; i < _vertices.Length - 1; i++)
                {
                    if (_lengths[i] < remaining)
                    {
                        remaining -= _lengths[i];
                    }
                    else
                    {
                        result = _vertices[i] + (_tangents[i] * remaining);
                        break;
                    }
                }
                return result;
            }
            internal FacilityDirection MapPathDistanceToDirection(double pathDistance)
            {
                if (pathDistance < 0)
                    return _tangents[0];
                if (pathDistance >= _totalLength)
                    return _tangents[_vertices.Length - 1];

                double remaining = pathDistance;

                // step through segments, subtracting off segment lengths until
                // locating the segment that contains the original pathDistance.
                // Return that segments direction.
                FacilityDirection result = new FacilityDirection(0, 0, 0);
                for (int i = 0; i < _vertices.Length - 1; i++)
                {
                    if (_lengths[i] < remaining)
                    {
                        remaining -= _lengths[i];
                    }
                    else
                    {
                        result = _tangents[i];
                        break;
                    }
                }
                return result;
            }
            private static double PointToSegmentDistance(FacilityLocation point, FacilityLocation ep0, FacilityLocation ep1, FacilityDirection segmentTangent, double segmentLength, out FacilityLocation chosen, out double segmentProjection)
            {
#if false
                // convert the test point to be "local" to ep0
                FacilityDirection local = (point - ep0).ToUnitLength();

                // find the projection of "local" onto "tangent"
                segmentProjection = segmentTangent.Dot(local);

                // handle boundary cases: when projection is not on segment, the
                // nearest point is one of the endpoints of the segment
                if (segmentProjection < 0)
                {
                    chosen = ep0;
                    segmentProjection = 0;
                    return Distance(point, ep0);
                }
                if (segmentProjection > segmentLength)
                {
                    chosen = ep1;
                    segmentProjection = segmentLength;
                    return Distance(point, ep1);
                }

                // otherwise nearest point is projection point on segment
                var chosenOffset = segmentTangent * segmentProjection;
                chosen = ep0 + chosenOffset;
                return Distance(point, chosen);
#else
                var v = ep1 - ep0;
                var w = point - ep0;

                var c1 = w.Dot(v);
                if (c1 <= 0.0) // Before p0
                {
                    chosen = ep0;
                    segmentProjection = 0;
                    return Distance(point, ep0);
                }
                var c2 = v.Dot(v);
                if (c2 <= c1) // After p1
                {
                    chosen = ep1;
                    segmentProjection = segmentLength;
                    return Distance(point, ep1);
                }

                var b = c1 / c2;
                var pB = ep0 + (v * b);
                chosen = pB;
                segmentProjection = (v * b).ComputeLength();
                return Distance(point, pB);
#endif
            }
            private static double Distance(FacilityLocation a, FacilityLocation b)
            {
                return (a - b).ComputeLength();
            }
        }

        public static FacilityDirection FollowPath(FollowPathInfo info, double predictionTime, double velocity, FacilityLocation currentLocation, FacilityDirection currentUnitDirection, out bool bOnPath)
        {
            var goalDir = FollowPathGoalDirection(info, predictionTime, velocity, currentLocation, currentUnitDirection, out bOnPath);

            // return steering to seek target
            var currentDirectionalVelocity = currentUnitDirection * velocity;
            var desiredVelocity = goalDir * currentDirectionalVelocity.ComputeLength();
            var steer = desiredVelocity - currentDirectionalVelocity;
            return steer;
        }
        public static FacilityDirection FollowPathGoalDirection(FollowPathInfo info, double predictionTime, double velocity, FacilityLocation currentLocation, FacilityDirection currentUnitDirection, out bool bOnPath)
        {
            // Be pessimistic about having to correct to get on the path
            bOnPath = false;

            // our goal will be offset from our path distance by this amount
            var pathDistanceOffset = predictionTime * velocity;

            // predict our future position
            var futurePosition = currentLocation + (currentUnitDirection * velocity * predictionTime);

            // measure distance along path of our current and predicted positions
            var nowPathDistance = info.MapPointToPathDistance(currentLocation);
            var futurePathDistance = info.MapPointToPathDistance(futurePosition);

            // are we facing in the correction direction?
            bool rightway = ((pathDistanceOffset > 0) ?
                                   (nowPathDistance < futurePathDistance) :
                                   (nowPathDistance > futurePathDistance));

            // find the point on the path nearest the predicted future position
            FacilityDirection tangent;
            double outside;
            FacilityLocation onPath = info.MapPointToPath(futurePosition, out tangent, out outside);

            var nowPathDirection = info.MapPathDistanceToDirection(nowPathDistance);

            FacilityLocation target = new FacilityLocation();

            // no steering is required if (a) our future position is inside
            // the path tube and (b) we are facing in the correct direction
            if ((outside < 0) && rightway)
            {
                bOnPath = true;
                target = currentLocation + nowPathDirection;
            }
            else
            {
                // otherwise we need to steer towards a target point obtained
                // by adding pathDistanceOffset to our current path position
                var targetPathDistance = nowPathDistance + (pathDistanceOffset * 4); // 4 is just a magic number so that things don't turn so "inward" when adjusting to stay on the path, but instead target a location further down
                target = info.MapPathDistanceToPoint(targetPathDistance);
            }

            var offset = target - currentLocation;
            var unitDir = offset.ToUnitLength();
            if (nowPathDirection.dY == 0.0)
                unitDir = new FacilityDirection(unitDir.dX, 0, unitDir.dZ).ToUnitLength();

            return unitDir; 
        }

        public static FacilityDirection SteerForSeekWithoutChangeInSpeed(FacilityLocation target, FacilityLocation currentLocation, FacilityDirection currentDirectionalVelocity)
        {
            var offset = target - currentLocation;
            var desiredVelocity = offset.ToUnitLength() * currentDirectionalVelocity.ComputeLength();
            return desiredVelocity - currentDirectionalVelocity;
        }

        public struct SeparateInfo
        {
            public SeparateInfo(FacilityLocation loc, double r)
            {
                Location = loc;
                Radius = r;
            }
            public readonly FacilityLocation Location;
            public readonly double Radius;
        }
        public static FacilityDirection Separate(SeparateInfo current, IEnumerable<SeparateInfo> others, double radiusSeparationRatio)
        {
            FacilityDirection steer = new FacilityDirection(0, 0, 0);

            int count = 0;
            foreach(var other in others)
            {
                var diff = current.Location - other.Location;
                diff = new FacilityDirection(diff.dX, 0.0, diff.dZ);
                var diffMagnitude = diff.ComputeLength();
                if (diffMagnitude > 0 && diffMagnitude < ((current.Radius + other.Radius) * radiusSeparationRatio))
                {
                    var steerAway = diff.ToUnitLength();
                    steerAway /= diffMagnitude;
                    steer += steerAway;
                    count++;
                }
            }

            return steer;
        }
    }
}
