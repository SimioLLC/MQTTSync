﻿using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Text;

namespace MQTTSync
{
    class MQTTPublishDefinition : IStepDefinition
    {
        #region IStepDefinition Members

        /// <summary>
        /// Property returning the full name for this type of step. The name should contain no spaces.
        /// </summary>
        public string Name
        {
            get { return "MQTTPublish"; }
        }

        /// <summary>
        /// Property returning a short description of what the step does.
        /// </summary>
        public string Description
        {
            get { return "MQTT Publish of 'payload' with 'topic'"; }
        }

        /// <summary>
        /// Property returning an icon to display for the step in the UI.
        /// </summary>
        public System.Drawing.Image Icon
        {
            get { return null; }
        }

        /// <summary>
        /// Property returning a unique static GUID for the step.
        /// </summary>
        public Guid UniqueID
        {
            get { return MY_ID; }
        }
        static readonly Guid MY_ID = new Guid("{49640c35-9a24-4d9d-8b1e-b28e7a2ed5c4}");

        /// <summary>
        /// Property returning the number of exits out of the step. Can return either 1 or 2.
        /// </summary>
        public int NumberOfExits
        {
            get { return 1; }
        }

        /// <summary>
        /// Method called that defines the property schema for the step.
        /// </summary>
        public void DefineSchema(IPropertyDefinitions schema)
        {
            // Example of how to add a property definition to the step.
            IPropertyDefinition pd;

            // Example of how to add an element property definition to the step.
            pd = schema.AddElementProperty("MQTTElement", MQTTElementDefinition.MY_ID);
            pd.DisplayName = "MQTTElement";
            pd.Description = "MQTTElement holding MQTT connection information";
            pd.Required = true;

            pd = schema.AddExpressionProperty("Topic", "Simio/PublishTopic");
            pd.DisplayName = "Topic";
            pd.Description = "MQTT Topic; by convention, it is hierarchical with slashes";
            pd.Required = true;

            pd = schema.AddExpressionProperty("Payload", String.Empty);
            pd.DisplayName = "Payload";
            pd.Description = "The payload data to publish to Topic";
            pd.Required = true;

            pd = schema.AddRealProperty("QualityOfService", 0);
            pd.DisplayName = "Quality Of Service";
            pd.Description = "Quality Of Service....0nly 0, 1 or 2 are valid...If invalid, 0 is selected";            
            pd.Required = true;

            pd = schema.AddBooleanProperty("RetainMessage");
            pd.DisplayName = "Retain Message";
            pd.Description = "Retain Message as 'last good message' for any client subscribing after publish";
            pd.DefaultString = "True";
            pd.Required = true;

            pd = schema.AddStateProperty("Response");
            pd.Description = "The string state where the response will be read into.";
            pd.Required = true;
        }

        /// <summary>
        /// Method called to create a new instance of this step type to place in a process.
        /// Returns an instance of the class implementing the IStep interface.
        /// </summary>
        public IStep CreateStep(IPropertyReaders properties)
        {
            return new MQTTPublish(properties);
        }

        #endregion
    }

    class MQTTPublish : IStep
    {
        IPropertyReaders _properties;
        IElementProperty _mqttElementProp;
        IPropertyReader _topicProp;
        IPropertyReader _payloadProp;
        IPropertyReader __qOSProp;
        IPropertyReader _retainMessageProp;
        IPropertyReader _responseProp;


        public MQTTPublish(IPropertyReaders properties)
        {
            _properties = properties;
            _mqttElementProp = (IElementProperty)_properties.GetProperty("MQTTElement");
            _topicProp = (IPropertyReader)_properties.GetProperty("Topic");
            _payloadProp = (IPropertyReader)_properties.GetProperty("Payload");
            __qOSProp = (IPropertyReader)_properties.GetProperty("QualityOfService");
            _retainMessageProp = (IPropertyReader)_properties.GetProperty("RetainMessage");
            _responseProp = (IPropertyReader)_properties.GetProperty("Response");
        }

        #region IStep Members

        /// <summary>
        /// Method called when a process token executes the step.
        /// </summary>
        public ExitType Execute(IStepExecutionContext context)
        {
            MQTTElement mqttElementProp = (MQTTElement)_mqttElementProp.GetElement(context);

            var payloadExpression = (IExpressionPropertyReader)_payloadProp;
            var payload = payloadExpression.GetExpressionValue((IExecutionContext)context).ToString();
            var topicExpression = (IExpressionPropertyReader)_topicProp;
            var topic = topicExpression.GetExpressionValue((IExecutionContext)context).ToString();
            double qOSDouble = __qOSProp.GetDoubleValue(context);
            int qOS = (int)Math.Floor(qOSDouble);
            double retainMessageDouble = _retainMessageProp.GetDoubleValue(context);
            bool retainMessage = false;
            if (retainMessageDouble > 0) retainMessage = true;
            IStateProperty responseStateProp = (IStateProperty)_responseProp;
            IState responseState = responseStateProp.GetState(context);
            IStringState responseStringState = responseState as IStringState;

            var response = mqttElementProp.PublishMessageAsync(topic, payload, qOS, retainMessage).Result;
            responseStringState.Value = response;

            context.ExecutionInformation.TraceInformation($"Published Topic : '{topic} - Published Payload :'{payload}' - Response :'{response}'");

            return ExitType.FirstExit;
        }

        #endregion
    }
}
