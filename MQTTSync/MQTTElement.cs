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
using System.Runtime.Remoting.Messaging;
using System.Collections;


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

            pd = schema.PropertyDefinitions.AddBooleanProperty("PersistSubscribedMessages");
            pd.DisplayName = "Persist Subscribed Messages";
            pd.Description = "Persist Subscribed Messages to be retreived using MQTTSubscriptionsIntoOutputTable step";
            pd.DefaultString = "False";
            pd.Required = true;

            _ed = schema.EventDefinitions.AddEvent("ElementEvent");
            _ed.Description = "An event owned by this element";

            pd = schema.PropertyDefinitions.AddRealProperty("MinEventFrequency", 0.5);
            pd.DisplayName = "Min Event Frequency(Seconds)";
            pd.Description = "Min Event Frequency..Min amount of time (in seconds) that element even will be fired";
            pd.Required = true;

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
        Double _minEventFrequency= Double.MaxValue;
        DateTime _lastEventDateTime = DateTime.MinValue;
        string _clientId = String.Empty;
        int _qos= 0;
        bool _update = false;
        string[] _topics;
        Dictionary<string, List<string>> _topicMessages = new Dictionary<string, List<string>>();
        bool _persistSubscribedMessages = false;

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
            _qos = Convert.ToInt32(qosProp.GetDoubleValue(_data.ExecutionContext));

            IPropertyReader persistSubscrivedMessagesProp = _data.Properties.GetProperty("PersistSubscribedMessages");
            _persistSubscribedMessages = Convert.ToBoolean(persistSubscrivedMessagesProp.GetDoubleValue(_data.ExecutionContext));

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

            IPropertyReader minEventFreqProp = _data.Properties.GetProperty("MinEventFrequency");
            _minEventFrequency = minEventFreqProp.GetDoubleValue(_data.ExecutionContext);

            if (_minEventFrequency <= 0) throw new Exception("Min Event Property Must Be Greater Than Zero");

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
            _value = Encoding.Default.GetString(arg.ApplicationMessage.Payload, 0, arg.ApplicationMessage.Payload.Length);

            if (_persistSubscribedMessages == true)
            {
                if (!_topicMessages.ContainsKey(_topic)) _topicMessages.Add(_topic, new List<string> { _value });
                else _topicMessages[_topic].Add(_value);
            }

            if (DateTime.Now >= _lastEventDateTime.AddSeconds(_minEventFrequency))
            {
                _lastEventDateTime = DateTime.Now;
                _update = true;

                _data.ExecutionContext.Calendar.ScheduleCurrentEvent(null, (obj) =>
                {
                    _data.Events["ElementEvent"].Fire();
                });
            }
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

        public string GetMessageToPublish(string[,] stringArray, string[] columnNames, int numOfRows)
        {
            // define data table
            DataTable dataTable = new DataTable();
            dataTable.Locale = System.Globalization.CultureInfo.InvariantCulture;
            int numberOfColumns = (int)(stringArray.Length / numOfRows);

            for (int i = 0; i < numberOfColumns; i++)
            {
                dataTable.Columns.Add(columnNames[i]);
            }

            DataTable[] dataTables = { dataTable };
            // define data row
            DataRow dataRow = null;

            // for each parameter
            for (int i = 0; i < numOfRows; i++)
            {
                for (int j = 0; j < numberOfColumns; j++)
                {
                    if (j == 0)
                    {
                        dataRow = dataTable.NewRow();
                    }
                    dataRow[j] = stringArray[i, j];
                }
                dataTable.Rows.Add(dataRow);
            }

            return JsonConvert.SerializeObject(dataTable);

        }

        public string[,] GetArrayOfSubscriptions(string topicName, string stylesheet, int numOfColumns, out string[,] stringArray, out int numOfRows)
        {
            var mergedDataSet = new DataSet();
            mergedDataSet.Locale = System.Globalization.CultureInfo.InvariantCulture;
            List<string> requestResults = new List<string>();

            // if topic exist
            if (_topicMessages.ContainsKey(topicName))
            {
                // if messages exist
                if (_topicMessages[topicName].Count > 0)
                {
                    //  local collection to avoid locking of public list
                    var receivedTopicMessages = _topicMessages[topicName].ToArray();
                    var numberOfMessages = receivedTopicMessages.Length;
                    _topicMessages[topicName].RemoveRange(0, numberOfMessages);
                 
                    foreach (string message in receivedTopicMessages)
                    {
                        requestResults.Add(MQTTElement.ParseDataToXML(message, out var parseError));
                        if (parseError.Length > 0)
                        {
                            throw new Exception(parseError);
                        }
                    }
                }                
            }

            if (requestResults.Count > 0)
            {
                foreach (var requestResult in requestResults)
                {
                    var transformedResult = Simio.Xml.XsltTransform.TransformXmlToDataSet(requestResult, stylesheet, null);
                    if (transformedResult.XmlTransformError != null)
                        throw new Exception(transformedResult.XmlTransformError);
                    if (transformedResult.DataSetLoadError != null)
                        throw new Exception(transformedResult.DataSetLoadError);
                    if (transformedResult.DataSet.Tables.Count > 0) numOfRows = transformedResult.DataSet.Tables[0].Rows.Count;
                    else numOfRows = 0;
                    if (numOfRows > 0)
                    {
                        transformedResult.DataSet.AcceptChanges();
                        if (mergedDataSet.Tables.Count == 0) mergedDataSet.Merge(transformedResult.DataSet);
                        else mergedDataSet.Tables[0].Merge(transformedResult.DataSet.Tables[0]);
                        mergedDataSet.AcceptChanges();
                    }
                    var xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(requestResult);
                }
            }

            if (mergedDataSet.Tables.Count == 0 || mergedDataSet.Tables[0].Rows.Count == 0)
            {
                numOfRows = 0;
                stringArray = new string[numOfRows, numOfColumns];
            }
            else
            {
                numOfRows = mergedDataSet.Tables[0].Rows.Count;
                stringArray = new string[numOfRows, numOfColumns];
                int rowNumber = -1;
                foreach (DataRow dataRow in mergedDataSet.Tables[0].Rows)
                {
                    rowNumber++;
                    for (int col = 0; col < mergedDataSet.Tables[0].Columns.Count; col++)
                    {
                        stringArray[rowNumber, col] = dataRow.ItemArray[col].ToString();
                    }
                }
            }

            return stringArray;
        }

        internal static string ParseDataToXML(string responseString, out string responseError)
        {
            responseError = String.Empty;

            // no response
            if (responseString.Length == 0) return responseString;

            bool isXMLResponse = false;
            bool isProbablyJSONObject = false;
            XmlDocument xmlDoc;
            if (responseString.Contains("xml"))
            {
                isXMLResponse = true;
            }
            else
            {
                isProbablyJSONObject = checkIsProbablyJSONObject(responseString);
            }

            if (isXMLResponse)
            {
                return responseString;
            }
            else // Default to assume a JSON response
            {
                xmlDoc = JSONToXMLDoc(responseString, isProbablyJSONObject);
            }

            return xmlDoc.InnerXml;
        }

        internal static bool checkIsProbablyJSONObject(string resultString)
        {
            // We are looking for the first non-whitespace character (and are specifically not Trim()ing here
            //  to eliminate memory allocations on potentially large (we think?) strings
            foreach (var theChar in resultString)
            {
                if (Char.IsWhiteSpace(theChar))
                    continue;

                if (theChar == '{')
                {
                    return true;
                }
                else if (theChar == '<')
                {
                    return false;
                }
                else
                {
                    break;
                }
            }
            return false;
        }

        internal static XmlDocument JSONToXMLDoc(string resultString, bool isProbablyJSONObject)
        {
            XmlDocument xmlDoc;
            resultString = resultString.Replace("@", string.Empty);
            if (isProbablyJSONObject == false)
            {
                var prefix = "{ items: ";
                var postfix = "}";

                using (var combinedReader = new StringReader(prefix)
                                            .Concat(new StringReader(resultString))
                                            .Concat(new StringReader(postfix)))
                {
                    var settings = new JsonSerializerSettings
                    {
                        Converters = { new Newtonsoft.Json.Converters.XmlNodeConverter() { DeserializeRootElementName = "data" } },
                        DateParseHandling = DateParseHandling.None,
                    };
                    using (var jsonReader = new JsonTextReader(combinedReader) { CloseInput = false, DateParseHandling = DateParseHandling.None })
                    {
                        xmlDoc = JsonSerializer.CreateDefault(settings).Deserialize<XmlDocument>(jsonReader);
                    }
                }
            }
            else
            {
                xmlDoc = Newtonsoft.Json.JsonConvert.DeserializeXmlNode(resultString, "data");
            }
            return xmlDoc;
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
