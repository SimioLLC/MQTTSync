using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Xml;
using SimioAPI;
using SimioAPI.Extensions;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet;
using MQTTnet.Server;
using System.Threading.Tasks;
using System.Runtime.Remoting.Contexts;
using System.Web;

namespace MQTTSync
{
    class MQTTElementDefinition : IElementDefinition
    {
        #region IElementDefinition Members

        /// <summary>
        /// Property returning the full name for this type of element. The name should contain no spaces.
        /// </summary>
        public string Name
        {
            get { return "MQTTElement"; }
        }

        /// <summary>
        /// Property returning a short description of what the element does.
        /// </summary>
        public string Description
        {
            get { return "MQTT broker connection information."; }
        }

        /// <summary>
        /// Property returning an icon to display for the element in the UI.
        /// </summary>
        public System.Drawing.Image Icon
        {
            get { return null; }
        }

        /// <summary>
        /// Property returning a unique static GUID for the element.
        /// </summary>
        public Guid UniqueID
        {
            get { return MY_ID; }
        }
        public static readonly Guid MY_ID = new Guid("{71404aaa-d689-4ea5-96d6-6b31b0221f83}");

        IEventDefinition _ed;

        /// <summary>
        /// Method called that defines the property, state, and event schema for the element.
        /// </summary>
        public void DefineSchema(IElementSchema schema)
        {
            // Example of how to add a property definition to the element.
            IPropertyDefinition pd = schema.PropertyDefinitions.AddStringProperty("Broker", "localhost");
            pd.Description = "Broker Name or IP Address";
            pd.Required = true;

            pd = schema.PropertyDefinitions.AddRealProperty("Port", 1883);
            pd.Description = "Port";
            pd.Required = true;

            pd = schema.PropertyDefinitions.AddExpressionProperty("ClientId", "Name");
            pd.Description = "ClientId";
            pd.Required = true;

            IRepeatGroupPropertyDefinition subscribeTopics = schema.PropertyDefinitions.AddRepeatGroupProperty("SubscribeTopics");
            subscribeTopics.Description = "Subscribe Topics.";
            pd = subscribeTopics.PropertyDefinitions.AddStringProperty("SubscribeTopic", String.Empty);
            pd.DisplayName = "Subscribe Topic";
            pd.Description = "Subscribe Topic.";
            pd.Required = true;

            pd = schema.PropertyDefinitions.AddRealProperty("QualityOfService", 0);
            pd.DisplayName = "Quality Of Service";
            pd.Description = "Quality Of Service....0nly 0, 1 or 2 are valid...If invalid, 0 is selected";
            pd.Required = true;

            _ed = schema.EventDefinitions.AddEvent("ElementEvent");
            _ed.Description = "An event owned by this element";

            schema.ElementFunctions.AddSimpleStringFunction("GetTopic", "GetTopicOfMessage", new SimioSimpleStringFunc(GetTopic));
            schema.ElementFunctions.AddSimpleStringFunction("GetStringValue", "GetStringValueOfMessage", new SimioSimpleStringFunc(GetStringValue));
        }

        public string GetTopic(object element)
        {
            var myElement = element as MQTTElement;
            return myElement.getTopic();
        }

        public string GetStringValue(object element)
        {
            var myElement = element as MQTTElement;
            return myElement.getStirngValue();
        }

        /// <summary>
        /// Method called to add a new instance of this element type to a model.
        /// Returns an instance of the class implementing the IElement interface.
        /// </summary>
        public IElement CreateElement(IElementData data)
        {
            return new MQTTElement(data);
        }

        #endregion
    }

    class MQTTElement : IElement
    {
        IElementData _data;
        public MqttFactory MQTTFactory = null;
        public IManagedMqttClient MQTTClient = null;
        String _topic = String.Empty;
        String _value = String.Empty;
        string _broker = String.Empty;
        Int32 _port = 0;
        string _clientId = String.Empty;
        int _qos= 0;
        bool _update = false;
        string[] _topics;

        public MQTTElement(IElementData data)
        {
            _data = data;
        }

        #region IElement Members

        /// <summary>
        /// Method called when the simulation run is initialized.
        /// </summary>
        public void Initialize()
        {
            IPropertyReader brokerProp = _data.Properties.GetProperty("Broker");
            _broker = brokerProp.GetStringValue(_data.ExecutionContext);

;           IPropertyReader portProp = _data.Properties.GetProperty("Port");
            _port = Convert.ToInt32(portProp.GetDoubleValue(_data.ExecutionContext));

            IPropertyReader clientIdProp = _data.Properties.GetProperty("ClientId");
            var clientIdExpression = (IExpressionPropertyReader)clientIdProp;
            _clientId = clientIdExpression.GetExpressionValue((IExecutionContext)_data.ExecutionContext).ToString();

            IPropertyReader qosProp = _data.Properties.GetProperty("QualityOfService");
            _qos = Convert.ToInt32(portProp.GetDoubleValue(_data.ExecutionContext));

            IRepeatingPropertyReader subscribeTopicsProp = (IRepeatingPropertyReader)_data.Properties.GetProperty("SubscribeTopics");

            int numInSubscribeTopicRepeatGroups = subscribeTopicsProp.GetCount(_data.ExecutionContext);
            _topics = new string[numInSubscribeTopicRepeatGroups];

            for (int i = 0; i < numInSubscribeTopicRepeatGroups; i++)
            {
                // The thing returned from GetRow is IDisposable, so we use the using() pattern here
                using (IPropertyReaders subscribeTopicsRow = subscribeTopicsProp.GetRow(i, _data.ExecutionContext))
                {
                    // Get the string property
                    IPropertyReader subscribeTopicProp = subscribeTopicsRow.GetProperty("SubscribeTopic");
                    _topics[i] = subscribeTopicProp.GetStringValue(_data.ExecutionContext);
                }
            }

            try
            {
                var subscribeError = SubscribeTopicsAsync().Result;
                if (subscribeError.Length > 0)
                {
                    throw new Exception(subscribeError);
                }
            }
            catch (Exception ex)
            {
                var topicsStr = String.Join(",", _topics);
                _data.ExecutionContext.ExecutionInformation.ReportError($"Could not Connect (is the Broker running?): ClientID={_clientId}. Topic={topicsStr}. Err={ex.Message}");
            }
        }

        internal async Task<String> SubscribeTopicsAsync()
        {
            var responseError = String.Empty;

            if (MQTTClient == null)
            {
                MQTTFactory = new MqttFactory();
                MQTTClient = MQTTFactory.CreateManagedMqttClient();

                // Create client options object
                MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                        .WithClientId(_clientId)
                                                        .WithTcpServer(_broker, _port);

                ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                        .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                        .WithClientOptions(builder.Build())
                                        .Build();

                await MQTTClient.StartAsync(options);

            }

            try
            {
                MQTTClient.ApplicationMessageReceivedAsync += MQTTClient_ApplicationMessageReceivedAsync;
                foreach (var topic in _topics)
                {
                    if (_qos == 0)
                        await MQTTClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce);
                    else if (_qos == 1)
                        await MQTTClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce);
                    else
                        await MQTTClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce);
                }
            }
            catch (Exception ex)
            {
                responseError = ex.Message;
            }
            return responseError;
        }

        internal async Task<String> UnSubscribeTopicsAndDisconnectAsync()
        {
            var responseError = String.Empty;

            if (MQTTClient != null)
            {          
                try
                {
                    MQTTClient.ApplicationMessageReceivedAsync -= MQTTClient_ApplicationMessageReceivedAsync;
                    foreach (var topic in _topics)
                    {
                        await MQTTClient.UnsubscribeAsync(topic);
                    }
                    MQTTClient.Dispose();
                    MQTTClient =  null;
                }
                catch (Exception ex)
                {
                    responseError = ex.Message;
                }
            }
            return responseError;
        }

        internal Task MQTTClient_ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            _topic = arg.ApplicationMessage.Topic;
            _value = Encoding.UTF8.GetString(arg.ApplicationMessage.Payload, 0, arg.ApplicationMessage.Payload.Length);
            _update = true;

            _data.ExecutionContext.Calendar.ScheduleCurrentEvent(null, (obj) =>
            {
                _data.Events["ElementEvent"].Fire();
            });
            return Task.CompletedTask;
        }

        public async Task<String> PublishMessageAsync(string topic, string message, int qos, bool retainMessage)
        {
            var response = String.Empty;

            try
            {
                if (MQTTClient == null)
                {
                    MQTTFactory = new MqttFactory();
                    MQTTClient = MQTTFactory.CreateManagedMqttClient();

                    // Create client options object
                    MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                            .WithClientId(_clientId)
                                                            .WithTcpServer(_broker, _port);
                    ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                            .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                            .WithClientOptions(builder.Build())
                                            .Build();

                    await MQTTClient.StartAsync(options);

                }

                byte[] bytes = Encoding.ASCII.GetBytes(message);

                if (qos == 0)
                    await MQTTClient.EnqueueAsync(topic, message, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce, retainMessage);
                else if (qos == 1)
                    await MQTTClient.EnqueueAsync(topic, message, MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce, retainMessage);
                else
                    await MQTTClient.EnqueueAsync(topic, message, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce, retainMessage);
                response = "Success";
            }
            catch (Exception ex)
            {
                response = ex.Message;
            }
            return response;
        }

        /// <summary>
        /// Method called when the simulation run is terminating.
        /// </summary>
        public void Shutdown()
        {
            var unsubscribeError = UnSubscribeTopicsAndDisconnectAsync().Result;
            if (unsubscribeError.Length > 0)
            {
                throw new Exception(unsubscribeError);
            }
        }

        public string getTopic()
        {
            return _topic;
        }

        public string getStirngValue()
        {
            return _value;
        }

        public bool getUpdate()
        {
            return _update;
        }

        public void setUpdate(bool update)
        {
            _update = update;
        }

        #endregion
    }
}
