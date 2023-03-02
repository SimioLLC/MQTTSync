using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MQTTSync
{
    internal class NewGuidDefinition : IStepDefinition
    {
        #region IStepDefinition Members

        /// <summary>
        /// Property returning the full name for this type of step. The name should contain no spaces.
        /// </summary>
        public string Name
        {
            get { return "NewGuid"; }
        }

        /// <summary>
        /// Property returning a short description of what the step does.
        /// </summary>
        public string Description
        {
            get { return "Description text for the 'NewGuid' step."; }
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
        static readonly Guid MY_ID = new Guid("{551451c5-adf4-45ec-9f3b-afabdbef300a}");

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
            pd = schema.AddStateProperty("NewGuid");
            pd.Description = "The state where the NewGuid will be read into.";
            pd.Required = true;
        }

        /// <summary>
        /// Method called to create a new instance of this step type to place in a process.
        /// Returns an instance of the class implementing the IStep interface.
        /// </summary>
        public IStep CreateStep(IPropertyReaders properties)
        {
            return new NewGuid(properties);
        }

        #endregion
    }

    internal class NewGuid : IStep
    {
        IPropertyReaders _properties;
        IPropertyReader _newGuidProp;

        public NewGuid(IPropertyReaders properties)
        {
            _properties = properties;
            _newGuidProp = (IPropertyReader)_properties.GetProperty("NewGuid");
        }

        #region IStep Members

        /// <summary>
        /// Method called when a process token executes the step.
        /// </summary>
        public ExitType Execute(IStepExecutionContext context)
        {
            IStateProperty statusCodeStateProp = (IStateProperty)_newGuidProp;
            IState stateStatusState = statusCodeStateProp.GetState(context);
            IStringState stringStatusState = stateStatusState as IStringState;
            stringStatusState.Value = Guid.NewGuid().ToString();

            // Example of how to display a trace line for the step.
            context.ExecutionInformation.TraceInformation(String.Format("The new guid value is '{0}'.", stringStatusState.Value));

            return ExitType.FirstExit;
        }

        #endregion
    }
}
