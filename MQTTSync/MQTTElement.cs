using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            get { return "Description text for the 'MQTTElement' element."; }
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

            schema.ElementFunctions.AddSimpleRealNumberFunction("GetValue", "GetValue", new SimioSimpleRealNumberFunc(GetValue));
            schema.ElementFunctions.AddSimpleRealNumberFunction("GetRealtimeDate", "GetRealtimeDate", new SimioSimpleRealNumberFunc(GetRealtimeDate));
        }

        public double GetValue(object element)
        {
            var myElement = element as MQTTElement;
            return myElement.getValue();
        }

        public double GetRealtimeDate(object element)
        {
            var myElement = element as MQTTElement;
            return myElement.getRealtimeDate();
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

        String _value = "0.0";
        String _prevValue = "0.0";

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
                _mqttClient.Connect(System.Environment.MachineName + "|" + subscribeTopic);
                if (subscribeTopic.Length > 0)
                {
                    _mqttClient.Subscribe(new[] { subscribeTopic }, new byte[] { MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE });
                    _mqttClient.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
                }
            }
        }

        public void publishMessage(string topic, string message)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(message);
            _mqttClient.Publish(topic, bytes, MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE, false);
        }

        private void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            _value = Encoding.UTF8.GetString(e.Message, 0, e.Message.Length);
            if (_value.ToLowerInvariant() == "on") _value = "1.0";
            else if (_value.ToLowerInvariant() == "off") _value = "0.0";
            else if (IsNumericFromTryParse(_value) == false)
            {
                if (_prevValue == "0.0") _value = "1.0";
                else _value = "0.0";
                _prevValue = _value;
            }
            _data.ExecutionContext.Calendar.ScheduleCurrentEvent(null, (obj) =>
            {
                _data.Events["ElementEvent"].Fire();
            });
        }

        private bool IsNumericFromTryParse(string str)
        {
            double result = 0;
            return (double.TryParse(str, System.Globalization.NumberStyles.Float,
                    System.Globalization.NumberFormatInfo.CurrentInfo, out result));
        }

        /// <summary>
        /// Method called when the simulation run is terminating.
        /// </summary>
        public void Shutdown()
        {
            _mqttClient.Disconnect();
            _mqttClient = null;
        }

        public double getValue()
        {
            return Convert.ToDouble(_value);
        }

        public double getRealtimeDate()
        {
            return DateTime.Now.ToOADate();
        }

        #endregion
    }
}
