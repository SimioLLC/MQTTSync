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
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

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
            IPropertyDefinition pd = schema.PropertyDefinitions.AddStringProperty("Broker", "10.0.0.192");
            pd.Description = "Broker Name or IP Address";
            pd.Required = true;           

            // Example of how to add a property definition to the element.
            pd = schema.PropertyDefinitions.AddStringProperty("SubscribeTopic", "Simio/SubscribeTopic");
            pd.Description = "Subscribe Topic";
            pd.Required = true;

            _ed = schema.EventDefinitions.AddEvent("ElementEvent");
            _ed.Description = "An event owned by this element";

            schema.ElementFunctions.AddSimpleStringFunction("GetStringValue", "GetStringValue", new SimioSimpleStringFunc(GetStringValue));
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
        MqttClient _mqttClient;
        String _value = String.Empty;
        bool _update = false;

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
            string broker = brokerProp.GetStringValue(_data.ExecutionContext);

            IPropertyReader subscribeTopicProp = _data.Properties.GetProperty("SubscribeTopic");
            string subscribeTopic = subscribeTopicProp.GetStringValue(_data.ExecutionContext);

            if (_mqttClient == null)
            {
                _mqttClient = new MqttClient(broker);
                _mqttClient.Connect(_data.ExecutionContext.ExecutionInformation.ResultSetId + "|" + subscribeTopic);
                if (subscribeTopic.Length > 0)
                {
                    _mqttClient.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
                    _mqttClient.Subscribe(new[] { subscribeTopic }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
                }
            }
        }

        public void publishMessage(string topic, string message, int qos, bool retainMessage)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(message);
            if (qos == 1) 
                _mqttClient.Publish(topic, bytes, MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, retainMessage);
            else if (qos == 2) 
                _mqttClient.Publish(topic, bytes, MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, retainMessage);
            else 
                _mqttClient.Publish(topic, bytes, MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE, retainMessage);
        }

        private void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            _value = Encoding.UTF8.GetString(e.Message, 0, e.Message.Length);
            _update = true;
            _data.ExecutionContext.Calendar.ScheduleCurrentEvent(null, (obj) =>
            {
                _data.Events["ElementEvent"].Fire();
            });
        }        

        /// <summary>
        /// Method called when the simulation run is terminating.
        /// </summary>
        public void Shutdown()
        {
            _mqttClient.Disconnect();
            _mqttClient = null;
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
