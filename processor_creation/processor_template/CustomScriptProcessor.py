# TODO 1: import the libraries needed for the script here
from typing import List
from pathlib import Path
#necessary imports for NiFi processor
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.relationship import Relationship
from nifiapi.properties import PropertyDescriptor, StandardValidators

#TODO 2: If the script is dependent on some functions that are defined in another script that is stored in the same directory as the processor, keep the folowing lines
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

# TODO 3: Rename this class to match your processor name (and also update the filename to match!).
class CustomScriptProcessor(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        # TODO 4: Update processor version, description, tags, and pip dependencies.
        version = '0.0.1-SNAPSHOT'
        description = "Converts incoming FlowFile data using custom Python logic."
        tags = ["custom", "python", "transform"]
        dependencies = []  # for example, ["pandas==2.2.2", "requests>=2.31.0"]

    # TODO 5: Change success and failure description depending on the function of the processor
    SUCCESS = Relationship(name='success', description='')
    FAILURE = Relationship(name='failure', description='')

    def __init__(self, *args, **kwargs):
        super().__init__()

        # TODO 6: For each of the parameters that the processor needs to have, use the code below changing the name and description
        # these parameters will be defined inside the processor. 
        # ATTENTION!! The values that are defined inside the processor will be prioritized over the parameters defined in the flowfile!!
        self.variable1 = PropertyDescriptor(
            name='',
            description='',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        ) 

        # TODO 7: Add all the defined parameters in the self.descriptors list
        self.descriptors = [self.variable1]

    def getPropertyDescriptors(self) -> List[PropertyDescriptor]:
        return self.descriptors

    def getRelationships(self):
        return [self.SUCCESS, self.FAILURE]

    # TODO 8: Add here all the functions that exist in the script. 
    # Take into account that all the functions that do not need access to 'self' need to have the @staticmethod tag over them
    @staticmethod
    def function1(variable1):
        """ Function from the script """
        sum = variable1 + variable1
        return sum


    def transform(self, context, flowfile) -> FlowFileTransformResult:
        """
        Runs once per incoming FlowFile.
        """

        # TODO 9: Add informative log to make the user know the processor is starting
        self.logger.info("================= xxx =================")

        if flowfile is None:
            self.logger.error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        # TODO 10: For each of the parameters the procesor needs to have, copy the following code changing the name
        # This way we make sure that the variable is taken from the processor property if it exists, and from the flowfile if it does not
        variable1 = context.getProperty(self.variable1).getValue()
        if variable1 is None or not variable1.strip():
            variable1 = flowfile.getAttribute("variable1") #add the attribute name as it appears in the flowfile
        if variable1 is None or not variable1.strip():
            self.logger.error(" variable1 missing from the processor property and FlowFile attribute 'variable1'")
            return FlowFileTransformResult(relationship="failure")

        
        try:

            # TODO 11: Add the execution of the script's main function
            # Remember to define all the script's function with the class's name before it
            result = CustomScriptProcessor.function1(variable1)

            # TODO 12: If necessary, add new attributes to the flowfile created by the script, to be able to use them in posterior processors
            return FlowFileTransformResult(
                relationship="success",
                attributes={
                    "newattribute": str(result)
                },
            )

        except Exception as e:
            import traceback
            # TODO 13: Add the description of the error when the processor fails
            self.logger.error(f"An error occurred when xxxx : {e}")
            traceback.print_exc()
            return FlowFileTransformResult(
                relationship="failure",
                attributes={"error": str(e)},
            )
