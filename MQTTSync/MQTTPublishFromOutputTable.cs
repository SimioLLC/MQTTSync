using System;
using System.Globalization;
using MQTTSync;
using Newtonsoft.Json.Linq;
using SimioAPI;
using SimioAPI.Extensions;

namespace MQTTSync
{
    public class MQTTPublishFromOutputTableDef : IStepDefinition
    {
        #region IStepDefinition Members

        /// <summary>
        /// Property returning the full name for this type of step. The name should contain no spaces. 
        /// </summary>
        public string Name
        {
            get { return "MQTTPublishFromOutputTable"; }
        }

        /// <summary>
        /// Property returning a short description of what the step does.  
        /// </summary>
        public string Description
        {
            get { return "The MQTTPublishFromOutputTable step may be used to write data to a database."; }
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
        static readonly Guid MY_ID = new Guid("{1d1b01ba-5519-48df-bf35-15f1d237bd1e}");

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
            IPropertyDefinition pd;

            pd = schema.AddElementProperty("MQTTElement", MQTTElementDefinition.MY_ID);
            pd.DisplayName = "MQTTElement";
            pd.Description = "MQTTElement holding MQTT connection information";
            pd.Required = true;

            pd = schema.AddTableReferenceProperty("SourceTable");
            pd.DisplayName = "Soruce Table";
            pd.Description = "Simio source table";
            pd.Required = true;

            pd = schema.AddBooleanProperty("ClearRowsAfterPublish");
            pd.DisplayName = "Clear Rows After Publish";
            pd.Description = "Clear Rows After Publish.";
            pd.DefaultString = "True";
            pd.Required = true;

            pd = schema.AddStringProperty("DestinationTopic",String.Empty);
            pd.DisplayName = "Destination Topc";
            pd.Description = "The MQTT Topic name where the data is to be written.";
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
            pd.Description = "The state where the response will be read into.";
            pd.Required = true;
        }

        /// <summary>
        /// Method called to create a new instance of this step type to place in a process. 
        /// Returns an instance of the class implementing the IStep interface.
        /// </summary>
        public IStep CreateStep(IPropertyReaders properties)
        {
            return new MQTTPublishFromOutputTable(properties);
        }

        #endregion
    }

    class MQTTPublishFromOutputTable : IStep
    {
        IPropertyReaders _props;
        ITableReferencePropertyReader _sourceTableReaderProp;
        IPropertyReader _clearRowsAfterPublishProp;
        IPropertyReader _destinationTopicProp;
        IElementProperty _mqttElementProp;
        IPropertyReader __qOSProp;
        IPropertyReader _retainMessageProp;
        IPropertyReader _responseProp;

        public MQTTPublishFromOutputTable(IPropertyReaders properties)
        {
            _props = properties;
            _mqttElementProp = (IElementProperty)_props.GetProperty("MQTTElement");
            _sourceTableReaderProp = (ITableReferencePropertyReader)_props.GetProperty("SourceTable");
            _clearRowsAfterPublishProp = (IPropertyReader)_props.GetProperty("ClearRowsAfterPublish");
            _destinationTopicProp = _props.GetProperty("DestinationTopic");
            __qOSProp = (IPropertyReader)_props.GetProperty("QualityOfService");
            _retainMessageProp = (IPropertyReader)_props.GetProperty("RetainMessage");
            _responseProp = (IPropertyReader)_props.GetProperty("Response");
        }

        #region IStep Members

        /// <summary>
        /// Method called when a process token executes the step.
        /// </summary>
        public ExitType Execute(IStepExecutionContext context)
        {
            MQTTElement mqttElementProp = (MQTTElement)_mqttElementProp.GetElement(context);
            ITableRuntimeData sourceTable = _sourceTableReaderProp.GetTableReference(context);
            double clearRowsAfterPublishDouble = _clearRowsAfterPublishProp.GetDoubleValue(context);
            bool clearRowsAfterPublish = false;
            if (clearRowsAfterPublishDouble > 0) clearRowsAfterPublish = true;
            String topic = _destinationTopicProp.GetStringValue(context);
            double qOSDouble = __qOSProp.GetDoubleValue(context);
            int qOS = (int)Math.Floor(qOSDouble);
            double retainMessageDouble = _retainMessageProp.GetDoubleValue(context);
            bool retainMessage = false;
            if (retainMessageDouble > 0) retainMessage = true;
            IStateProperty responseStateProp = (IStateProperty)_responseProp;
            IState responseState = responseStateProp.GetState(context);
            IStringState responseStringState = responseState as IStringState;

            int numOfRows = sourceTable.GetCount(context);
            int numOfPropertyColumns = sourceTable.Table.Columns.Count;
            int numOfColumns = numOfPropertyColumns + sourceTable.Table.StateColumns.Count;

            string[] columnNames = new string[numOfColumns];
            object[,] paramsArray = new object[numOfRows, numOfColumns];

            // an array of string values from the repeat group's list of strings
            for (int i = 0; i < numOfRows; i++)
            {
                ITableRuntimeDataRow row = sourceTable.GetRow(i, context);

                for (int j = 0; j < sourceTable.Table.Columns.Count; j++)
                {
                    if (i == 0)
                    {
                        columnNames[j] = sourceTable.Table.Columns[j].Name;
                    }
                    paramsArray[i, j] = row.Properties.GetProperty(sourceTable.Table.Columns[j].Name).GetStringValue(context);
                }

                for (int j = 0; j < sourceTable.Table.StateColumns.Count; j++)
                {
                    if (i == 0)
                    {
                        columnNames[numOfPropertyColumns + j] = sourceTable.Table.StateColumns[j].Name;
                    }
                    paramsArray[i, numOfPropertyColumns + j] = TryReadState(row.States[j]);
                }  
            }

            try
            {
                // for each parameter
                string[,] stringArray = new string[numOfRows, numOfColumns];
                for (int i = 0; i < numOfRows; i++)
                {
                    for (int j = 0; j < numOfColumns; j++)
                    {
                        double doubleValue = paramsArray[i, j] is double ? (double)paramsArray[i, j] : Double.NaN;
                        if (!System.Double.IsNaN(doubleValue))
                        {
                            stringArray[i, j] = (Convert.ToString(doubleValue, CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            DateTime datetimeValue = TryAsDateTime((Convert.ToString(paramsArray[i, j], CultureInfo.InvariantCulture)));
                            if (datetimeValue > System.DateTime.MinValue)
                            {
                                stringArray[i, j] = (Convert.ToString(datetimeValue, CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                stringArray[i, j] = (Convert.ToString(paramsArray[i, j], CultureInfo.InvariantCulture));
                            }
                        }
                    }
                }

                if (numOfRows > 0)
                {
                    var payload = mqttElementProp.GetMessageToPublish(stringArray, columnNames, numOfRows);
                    var response = mqttElementProp.PublishMessageAsync(topic, payload, qOS, retainMessage).Result;
                    responseStringState.Value = response;
                    context.ExecutionInformation.TraceInformation($"Published Topic : '{topic} - Published Payload :'{payload}' - Response :'{response}'");
                }

                if (clearRowsAfterPublish == true) sourceTable.RemoveAllRows(context);

                return ExitType.FirstExit;
            }
            catch (FormatException)
            {
                context.ExecutionInformation.ReportError("Bad format provided in DbWrite step.");
            }

            // We are done writing, have the token proceed out of the primary exit
            return ExitType.FirstExit;
        }      

        string TryReadState(IState state)
        {   
            IStringState stringState = state as IStringState;
            if (stringState == null) return String.Empty;
            return stringState.Value;
        }

        DateTime TryAsDateTime(string rawValue)
        {
            DateTime dt = System.DateTime.MinValue;
            if (DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
            {
                return dt;
            }
            else
            {
                return dt;
            }
        }

        #endregion
    }
}
