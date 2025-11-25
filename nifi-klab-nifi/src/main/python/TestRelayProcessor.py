from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.documentation import use_case
from nifiapi.properties import PropertyDescriptor, StandardValidators
import sys
sys.path.append("/tmp/custom")
from klab_nifi_py import *



@use_case("Creates a strucutured flowfile for submitting observation ")
class TestRelayProcessor(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = '''A Python processor that creates FlowFiles.'''
        tags = ["Python", "FlowFile Create"]
        dependencies = []

    def __init__(self, **kwargs):

        ##super().__init__(**kwargs)

        ## Required since, klab can only be queried with semantics
        self.semantics = PropertyDescriptor(name="Semantics",
            description="Semantics of the Observation to be submitted to the Observation Relay Processor (Eg: geography:Elevation or geography:Aspect)",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True)
        
        self.timeYearSTart = PropertyDescriptor(name="Start Year",
            description="Time of the Observation to be submitted to the Observation Relay Processor (Eg: 2023-01-01T00:00:00Z)",
            )
        
        self.timeYearEnd = PropertyDescriptor(name="End Year",
            description="End Time of the Observation to be submitted to the Observation Relay Processor (Eg: 2023-12-31T23:59:59Z)",
            )
        
        self.geometry = PropertyDescriptor(name="Geometry",
            description="Geometry of the Observation to be submitted to the Observation Relay Processor (Eg: POINT(1 2))",
            )
        
        self.dtUrl = PropertyDescriptor(name="Digital Twin URL",
            description="URL of the Digital Twin instance to submit the Observation to",
            validators=[StandardValidators.URL_VALIDATOR, StandardValidators.NON_EMPTY_VALIDATOR],
            required=True)
        
        self.obsName = PropertyDescriptor(name="Observation Name",
            description="Name of the Observation to be submitted to the Observation Relay Processor",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True)
        
        self.descriptors = [self.semantics, self.timeYearSTart, self.timeYearEnd, self.geometry, self.dtUrl]


    def getPropertyDescriptors(self):
        return self.descriptors

    def create(self, context):

        space = Space(
            self.geometry
        )

        dt_2020 = datetime(self.timeYearSTart, 1, 1, 0, 0, 0)
        dt_2021 = datetime(self.timeYearEnd, 12, 31, 23, 59, 59)


        time = Time(
            tstart=dt_2020,
            tend = dt_2021
        )


        klabNifiObs = KlabObservationNifiRequest(
            space = space, 
            time = time,
            observationSemantics= "geography:Elevation",
            asContext=True,
            observationName="dillli",
            dtURL="https://services.integratedmodelling.org/runtime/main/dt/ESA_INSTITUTIONAL.knbrfrcjjl",
            id = -1,
        )


        return FlowFileSourceResult(relationship = 'success', contents = klabNifiObs.to_json())