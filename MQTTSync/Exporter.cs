﻿using SimioAPI.Extensions;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Data;
using System.Globalization;
using System.Xml.Serialization;
using Newtonsoft.Json;
using System.Net.NetworkInformation;
using System.Net;

namespace MQTTSync
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "MQTT Exporter";
        public string Description => "An exporter for row data to be sent as requests over MQTT";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("4b55856e-f5f1-409d-9093-f1a8dcf407ba");
        public Guid UniqueID => MY_ID;
        public IGridDataOverallSettings Settings { get; set; }

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var brokerProp = schema.OverallProperties.AddStringProperty("Broker");
            brokerProp.DisplayName = "Broker";
            brokerProp.Description = "Broker Name or IP Address.";
            brokerProp.DefaultValue = String.Empty;

            var portProp = schema.OverallProperties.AddRealProperty("Port");
            portProp.DisplayName = "Port";
            portProp.Description = "Port.";
            portProp.DefaultValue = 1883;

            var messageProp = schema.OverallProperties.AddStringProperty("Message");
            messageProp.DisplayName = "Message";
            messageProp.Description = "The message to post if Method is set to POST. Can contain tokens (in the form ${name}, or of the form ${col:Name} to use column values by colum name) to be later replaced by per-table settings.";
            messageProp.DefaultValue = String.Empty;

            var exportStartTimeStringProp = schema.OverallProperties.AddStringProperty("ExportStartTimeString");
            exportStartTimeStringProp.DisplayName = "Export Start Time String";
            exportStartTimeStringProp.Description = "Used to specify the export start time based on the models regional setting.  If left blank or valid, the time of the computer will be used.";
            exportStartTimeStringProp.DefaultValue = "${datetimeregionnow:s:0}";

            var topicProp = schema.PerTableProperties.AddStringProperty("Topic");
            topicProp.DisplayName = "Topic";
            topicProp.Description = "Subscribe Topic.";
            topicProp.DefaultValue = String.Empty;

            var qualityOfServiceProp = schema.PerTableProperties.AddListProperty("QualityOfService", MQTTSyncUtils.QUALITY_OF_SERVICE);
            qualityOfServiceProp.DisplayName = "Quality Of Service";
            qualityOfServiceProp.Description = "Quality Of Service.";
            qualityOfServiceProp.DefaultValue = MQTTSyncUtils.QUALITY_OF_SERVICE[0].ToString();

            var retainMessageProp = schema.PerTableProperties.AddBooleanProperty("RetainMessage");
            retainMessageProp.DisplayName = "Retain Message";
            retainMessageProp.Description = "Retain Message.";
            retainMessageProp.DefaultValue = true;

            var tableTokenReplacementsProp = schema.PerTableProperties.AddNameValuePairsProperty("TokenReplacements", null);
            tableTokenReplacementsProp.DisplayName = "Token Replacements";
            tableTokenReplacementsProp.Description = "Values used for tokens specified in either the URL or Message of the Data Connector. The values here can themselves contains tokens of the form ${col:Name} to use column values by colum name.";
            tableTokenReplacementsProp.DefaultValue = String.Empty;

            var exportTypePropTableProperties = schema.PerTableProperties.AddListProperty("ExportType", MQTTSyncUtils.EXPORT_TYPE);
            exportTypePropTableProperties.DisplayName = "Export Type";
            exportTypePropTableProperties.Description = "Defined the type of export.   If COLUMNMAPPING, the export will map columns defined in the URL, Form Parameters, " +
                "Message and Row Message to current row in the data table.  This type is useful if the required message needed for the destination is different than the structure " +
                "of the source table/log.  COLUMNMAPPING use the ${rowmessage} token replacement to define where in the message the rows should be placed. COLUMNMAPPING does not require the " +
                "${rowmessage} token replacement...The column mapping can be placed in the URL, Form Parameters or Message, but this approarch only works if the rows per call is set to 1. JSONOBJECT " +
                "will convert the source table/log into a JSON Object and send the JSON Object.  JSONObject use the ${jsonobject} token replacement to define where in the message the rows should be placed. " +
                "JSONARRAY will convert the source table/log into a JSON Array.    JSONARRAY use the ${jsonarray} token replacement to define where in the message the rows should be placed."; 
            exportTypePropTableProperties.DefaultValue = MQTTSyncUtils.EXPORT_TYPE[0].ToString();

            var statusFileNameProp = schema.PerTableProperties.AddFileProperty("StatusFileName");
            statusFileNameProp.DisplayName = "Status File Name";
            statusFileNameProp.Description = "Status File Name.";
            statusFileNameProp.DefaultValue = String.Empty;

            var statusDelimiterProp = schema.PerTableProperties.AddStringProperty("StatusDelimiter");
            statusDelimiterProp.DisplayName = "Status Delimiter";
            statusDelimiterProp.Description = "Status Delimiter.";
            statusDelimiterProp.DefaultValue = ";";

            var statusSeverityProp = schema.PerTableProperties.AddListProperty("StatusSeverity", MQTTSyncUtils.STATUS_SEVERITY);
            statusSeverityProp.DisplayName = "Status Severity";
            statusSeverityProp.Description = "Status contains Errors and Warnings (ERRORSANDWARNINGS) or Everything (ALL).";
            statusSeverityProp.DefaultValue = MQTTSyncUtils.STATUS_SEVERITY[0];

            var responseDebugFileFolderProp = schema.PerTableProperties.AddFilesLocationProperty("ResponseDebugFileFolder");
            responseDebugFileFolderProp.DisplayName = "Response Debug File Folder";
            responseDebugFileFolderProp.Description = "Response Debug File Folder.";
            responseDebugFileFolderProp.DefaultValue = String.Empty;
        }
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            var mySettings = openContext.Settings;
            var overallSettings = openContext.Settings.Properties;
            var table = openContext.GridDataName;
            var exportStartTimeString = (string)overallSettings?["ExportStartTimeString"]?.Value;
            var exportStartTime = DateTime.MinValue;
            if (DateTime.TryParse(exportStartTimeString, out exportStartTime)) { }
            else exportStartTime = DateTime.Now;
            var exportStartTimeOffsetHours = (exportStartTime - DateTime.Now).TotalHours;
            var tableSettings = openContext.Settings.GridDataSettings[openContext.GridDataName]?.Properties;
            int currentRowNumber = 0;
            var broker = (string)overallSettings?["Broker"]?.Value;
            var port = Convert.ToInt32((double)overallSettings?["Port"]?.Value);
            var message = (string)overallSettings?["Message"]?.Value;
            var topic = (string)tableSettings?["Topic"]?.Value;
            var qualityOfService = (string)tableSettings?["QualityOfService"]?.Value;
            var retainMessage = (bool)tableSettings?["RetainMessage"]?.Value;
            var requestDebugFileFolder = (string)tableSettings?["RequestDebugFileFolder"]?.Value;
            var exportType = (string)tableSettings?["ExportType"]?.Value;
            var statusFileName = (string)tableSettings?["StatusFileName"]?.Value;
            var statusDelimiter = (string)tableSettings?["StatusDelimiter"]?.Value;
            var statusSeverity = (string)tableSettings?["StatusSeverity"]?.Value;
            var responseDebugFileFolder = (string)tableSettings?["ResponseDebugFileFolder"]?.Value;

            // Create the log message list
            List<String> errorLog = new List<String>();

            // setup datatable
            DataTable dataTable = dataTable = new DataTable();
            dataTable.TableName = openContext.GridDataName;
            dataTable.Locale = CultureInfo.InvariantCulture;
            if (exportType.ToUpper() == "JSONOBJECT")
            {
                foreach (var col in openContext.Records.Columns)
                {
                    var dtCol = dataTable.Columns.Add(col.Name, col.Type);
                }
            }

            // setup dataArray
            List<object[]> dataArray = new List<object[]>();

            int numberOfColumns = openContext.Records.Columns.Count();
            int numberOfRows = openContext.Records.Count();

            var tokenReplacementsStr = (string)tableSettings?["TokenReplacements"]?.Value;
            var tokenReplacements = AddInPropertyValueHelper.NameValuePairsFromString(tokenReplacementsStr);

            var finalMessage = String.Empty;
            var currentMessage = String.Empty;

            foreach (var record in openContext.Records)
            {
                currentRowNumber++;
                var thisRecordValues = new Dictionary<string, string>();

                try
                {
                    if (exportType.ToUpper() == "COLUMNMAPPING")
                    {
                        int colIndex = 0;
                        foreach (var col in openContext.Records.Columns)
                        {
                            thisRecordValues[col.Name] = GetFormattedStringValue(record, colIndex);
                            colIndex++;
                        }

                        finalMessage = TokenReplacement.ResolveString(message, tokenReplacements, thisRecordValues);
                        var finalTopic = TokenReplacement.ResolveString(topic, tokenReplacements, thisRecordValues);

                        string traceText = "Broker:" + broker + " - Topic:" + finalTopic + " - Message:" + finalMessage;
                        System.Diagnostics.Trace.TraceInformation(traceText);
                        var responseError = MQTTSyncUtils.PublishMessageAsync(table, broker, port, finalTopic, finalMessage, qualityOfService, retainMessage).Result;

                        if (statusFileName.Length > 0 && (statusSeverity.ToLower().Contains("all") || responseError.Length > 0))
                        {
                            MQTTSyncUtils.logStatus(openContext.GridDataName, statusFileName, traceText, responseError, statusDelimiter, exportStartTimeOffsetHours);
                        }
                        if (responseError.Length > 0) throw new Exception(responseError);
                    }
                    else
                    {
                        if (exportType.ToUpper() == "JSONOBJECT")
                        {
                            object[] thisRow = new object[numberOfColumns];
                            int columnIndex = 0;
                            foreach (var col in openContext.Records.Columns)
                            {
                                string formattedValue = GetFormattedStringValue(record, columnIndex);
                                thisRow[columnIndex] = formattedValue;
                                columnIndex++;
                            }

                            dataTable.Rows.Add(thisRow);
                        }
                        else
                        {
                            // JSONARRAY
                            object[] thisRow = new object[numberOfColumns];
                            int columnIndex = 0;
                            foreach (var col in openContext.Records.Columns)
                            {
                                thisRow[columnIndex] = record.GetNativeObject(columnIndex);
                                columnIndex++;
                            }

                            dataArray.Add(thisRow);
                        }

                        if (currentRowNumber >= numberOfRows)
                        {
                            if (exportType.ToUpper() == "JSONOBJECT")
                            {
                                currentMessage = JsonConvert.SerializeObject(dataTable);
                                dataTable.Clear();
                                var rowCallTokenReplacement = new Dictionary<string, string>()
                                {
                                    ["jsonobject"] = currentMessage
                                };
                                finalMessage = TokenReplacement.ResolveString(message, tokenReplacements, null);
                                finalMessage = TokenReplacement.ResolveString(finalMessage, rowCallTokenReplacement, null);
                            }
                            else
                            {
                                //JSONARRAY
                                currentMessage = JsonConvert.SerializeObject(dataArray);
                                dataArray.Clear();
                                var rowCallTokenReplacement = new Dictionary<string, string>()
                                {
                                    ["jsonarray"] = currentMessage
                                };
                                finalMessage = TokenReplacement.ResolveString(message, tokenReplacements, null);
                                finalMessage = TokenReplacement.ResolveString(finalMessage, rowCallTokenReplacement, null);
                            }

                            string traceText = "Broker:" + broker + " - Topic:" + topic + " - Message:" + finalMessage;
                            System.Diagnostics.Trace.TraceInformation(traceText);
                            var responseError = MQTTSyncUtils.PublishMessageAsync(table, broker, port, topic, finalMessage, qualityOfService, retainMessage).Result;
                            if (statusFileName.Length > 0 && (statusSeverity.ToLower().Contains("all") || responseError.Length > 0))
                            {
                                MQTTSyncUtils.logStatus(openContext.GridDataName, statusFileName, traceText, responseError, statusDelimiter, exportStartTimeOffsetHours);
                            }
                            if (responseError.Length > 0) throw new Exception(responseError);
                        }
                    }  
                }
                catch (Exception e)
                {
                    var error = $"There was an error attempting to connect to Broker: '{broker}' on Topic: {topic}.  Response: {e.Message}";
                    errorLog.Add(error);
                }                
            }

            //var disconnectError = MQTTSyncUtils.DisconnectAsync();
            //if (disconnectError.Length > 0)
            //{
            //    errorLog.Add(disconnectError);
            //}

            if (errorLog.Count == 0) return OpenExportDataResult.Succeeded();
            else
            {
                string errors = string.Join(System.Environment.NewLine, errorLog.ToArray());

                string traceErrors = "Errors:" + errors;
                System.Diagnostics.Trace.TraceError(traceErrors);

                return OpenExportDataResult.Failed(errors);
            }
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string folderName, out string fileName, out bool bUseHeaders, out string separator, out string culture)
        {
            fileName = (string)settings.GridDataSettings[tableName]?.Properties["FileName"]?.Value;
            bUseHeaders = (bool)settings.Properties["WriteHeaders"].Value;
            folderName = (string)settings.Properties["ExportFolder"]?.Value; // ExportFolder was not defined in previous versions, hence the ?. null-conditional check here
            var separatorstr = (string)settings.Properties["Separator"].Value;
            if (String.IsNullOrWhiteSpace(separatorstr))
                separatorstr = ",";
            separator = separatorstr;
            culture = (string)settings.Properties["ExportCulture"].Value;
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            var broker = (string)context.Settings.Properties["Broker"]?.Value;
            var port = Convert.ToInt32((double)context.Settings.Properties["Port"]?.Value);
            var tokenReplacementsStr = (string)context.Settings.GridDataSettings[context.GridDataName]?.Properties["TokenReplacements"]?.Value;
            var tokenReplacements = AddInPropertyValueHelper.NameValuePairsFromString(tokenReplacementsStr);

            return $"Bound to Broker: {TokenReplacement.ResolveString(broker, tokenReplacements, null) + ":" + port.ToString()?? "[None]"}";
        }

        public void Dispose()
        {
        }

        private static string GetFormattedStringValue(IGridDataExportRecord record, Int32 colIdx)
        {
            var valueStr = record.GetString(colIdx) ?? "null";

            // Special handle certain types, to make sure they follow the export culture
            var valueObj = record.GetNativeObject(colIdx);
            if (valueObj is DateTime)
            {
                valueStr = String.Format(CultureInfo.InvariantCulture, "{0}", valueObj);
            }
            else if (valueObj is double valueDouble)
            {
                if (Double.IsPositiveInfinity(valueDouble))
                    valueStr = "Infinity";
                else if (Double.IsNegativeInfinity(valueDouble))
                    valueStr = "-Infinity";
                else if (Double.IsNaN(valueDouble))
                    valueStr = "NaN";
                else
                    valueStr = String.Format(CultureInfo.InvariantCulture, "{0}", valueDouble);
            }
            return valueStr;
        }       
    }
}
