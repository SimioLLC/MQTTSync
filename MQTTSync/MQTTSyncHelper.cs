using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Xml;

namespace MQTTSync
{
    public static class MQTTSyncHelper
    {
        internal static bool TryGetPositiveDoubleValue(IExpressionPropertyReader expressionReader, IExecutionContext context, out double theValue)
        {
            theValue = Double.NaN;
            var valueObj = expressionReader.GetExpressionValue(context);
            if (!(valueObj is double))
            {
                context.ExecutionInformation.ReportError(expressionReader as IPropertyReader, "Did not return a double value.");
                return false;
            }

            theValue = (double)valueObj;

            if (theValue <= 0.0 || Double.IsNaN(theValue) || Double.IsInfinity(theValue))
            {
                context.ExecutionInformation.ReportError(expressionReader as IPropertyReader, "Invalid value. Value must be a real number greater than zero.");
                return false;
            }

            return true;
        }

        internal static bool TryGetDoubleValue(IExpressionPropertyReader expressionReader, IExecutionContext context, out double theValue)
        {
            theValue = Double.NaN;
            var valueObj = expressionReader.GetExpressionValue(context);
            if (!(valueObj is double))
            {
                context.ExecutionInformation.ReportError(expressionReader as IPropertyReader, "Did not return a double value.");
                return false;
            }

            theValue = (double)valueObj;

            if (Double.IsNaN(theValue) || Double.IsInfinity(theValue))
            {
                context.ExecutionInformation.ReportError(expressionReader as IPropertyReader, "Invalid value. Value must be a real number greater than zero.");
                return false;
            }

            return true;
        }

        internal static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
    }
}
