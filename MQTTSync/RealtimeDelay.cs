using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MQTTSync
{
    class RealtimeDelayDefinition : IStepDefinition
    {
        #region IStepDefinition Members

        /// <summary>
        /// Property returning the full name for this type of step. The name should contain no spaces.
        /// </summary>
        public string Name
        {
            get { return "RealtimeDelay"; }
        }

        /// <summary>
        /// Property returning a short description of what the step does.
        /// </summary>
        public string Description
        {
            get { return "Synchronizes world (real) time with simulation time using a delay."; }
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
        static readonly Guid MY_ID = new Guid("{0ed90338-8a10-417e-9731-b05adf18f3da}");

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
            pd = schema.AddExpressionProperty("Seconds", "0.0");
            pd.DisplayName = "Seconds";
            pd.Description = "Seconds Desc.";
            pd.Required = true;
        }

        /// <summary>
        /// Method called to create a new instance of this step type to place in a process.
        /// Returns an instance of the class implementing the IStep interface.
        /// </summary>
        public IStep CreateStep(IPropertyReaders properties)
        {
            return new RealtimeDelay(properties);
        }

        #endregion
    }

    class RealtimeDelay : IStep
    {
        IPropertyReaders _properties;
        IPropertyReader _secondProp;

        public RealtimeDelay(IPropertyReaders properties)
        {
            _properties = properties;
            _secondProp = (IPropertyReader)_properties.GetProperty("Seconds");
        }

        #region IStep Members

        /// <summary>
        /// Method called when a process token executes the step.
        /// </summary>
        public ExitType Execute(IStepExecutionContext context)
        {
            var secondsExpression = (IExpressionPropertyReader)_secondProp;
            double seconds = (double)secondsExpression.GetExpressionValue((IExecutionContext)context);

            if (seconds > 0)
            {
                // This will suspend the thread for the given number of milliseconds, which
                // allows the simulation time to sync with world time.
                System.Threading.Thread.Sleep((int)(1000.0 * (seconds)));
            }

            // Example of how to display a trace line for the step.
            context.ExecutionInformation.TraceInformation($"The value of real time delay is '{seconds}' seconds.");

            return ExitType.FirstExit;
        }

        #endregion
    }
}
