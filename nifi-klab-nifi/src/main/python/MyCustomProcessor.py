from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.documentation import use_case
from nifiapi.properties import PropertyDescriptor, StandardValidators



@use_case("Converts text to uppercase for data normalization.")
class MyCustomProcessor(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = '''A Python processor that creates FlowFiles.'''
        tags = ["Python", "FlowFile Create"]
        dependencies = []

    def __init__(self, **kwargs):

        ##super().__init__(**kwargs)

        numspaces = PropertyDescriptor(name="Number of Spaces",
            description="Number of spaces to use for pretty-printing",
            validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
            default_value="4",
            required=True)
        
        self.descriptors = [numspaces]

    def getPropertyDescriptors(self):
        return self.descriptors

    def create(self, context):
        return FlowFileSourceResult(relationship = 'success', attributes = {'greeting': 'hello'}, contents = 'Hello World!')