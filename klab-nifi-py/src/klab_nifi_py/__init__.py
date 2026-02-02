from .geometry import *
from urllib.parse import urlparse
from .contextualizer import Contextualizer
from .logging import logger
import requests
import logging


NIFI_HEALTHCHECK_PATH = "/healthcheck"
CONTEXT_OBSERVATION_SEMANTICS = "earth:Terrestrial earth:Region"

class KlabObservationNifiRequest(BaseModel):
    '''
    The Main Observation Class in Python for creating the JSON Payload passing to 
    the Observation Relay Processor through the flowfile. use the method `to_dict()`
    method to convert the observation object to an equivalent JSON.

    If using the ListenHTTP Processor in Apache Nifi, use the :class:`Client` class, and use the 
    `submit(:class:NifiKlabObservation)` method.

    '''

    def __init__(self, 
                 observationName:str=None,
                 observationSemantics:str=None,
                 observationNamespace:str=None,
                 asContext:bool=False,
                 resetContext:bool=False,
                 space:Space=None,
                 time:Time=None,
                 dtURL:str=None,
                 contextualizer:Contextualizer=None,
                 loglevel:str=logging.INFO):
        
        logger.debug("KLAB Nifi Observation Initialized")
        logger.setLevel(loglevel)

        if dtURL :
            logger.debug("Validating and Setting the Digital Twin URL")
            parsed = urlparse(dtURL)
            if not all([parsed.scheme, parsed.netloc]):
                raise KlabNifiException("Digital Twin URL is not valid")
            
            self.digitalTwin = dtURL
        else:
            raise KlabNifiException("Digital Twin URL cannot be Null for Observation Request")

        if resetContext and (space or time or contextualizer or asContext or observationName or observationSemantics):
            raise KlabNifiException("Reset Context cannot be set along with other Observation Parameters")

        if resetContext:
            logger.info("Resetting Context for the Observation, Context Observation must be made prior to the future observations submitted")
            self.resetContext = True
            return

        ## The Identity of a Substantial Observation i.e. Observations that can exist independently
        ## necessarily need a Name, Namespace, and the ID is set as <Namespace>:<Name>

        if (not observationName or not observationNamespace) and asContext:
            raise KlabNifiException("Observation Name or Namespace is missing which is required for Context Observations")
        else:
            if observationName:
                logger.info("Setting Name to the Observation to " + observationName)
                self.name = observationName

        if observationNamespace:
            logger.info("Setting Namespace to the Observation to " + observationNamespace)
            self.namespace = observationNamespace
        
        if not observationSemantics:
            raise KlabNifiException("Observation Query must be made with a Semantics")

        if contextualizer :
            logger.info("Setting Contextualizer to the Observation")
            self.contextualizer = contextualizer


        ##TODO: check how can we validate the semantics here without the Python Client
        ## Keeping it as it is for now
        self.semantics = observationSemantics 
        self.asContext = asContext

        if contextualizer and asContext:
            raise KlabNifiException("Contextualizer cannot be set for Context Observations")

        if self.asContext and (not space or not time or observationSemantics != CONTEXT_OBSERVATION_SEMANTICS) :
            raise KlabNifiException("Context Observations must have both Space and Time defined, "
            "and Semantics must be set to " + CONTEXT_OBSERVATION_SEMANTICS)

        if space and time :
            logger.debug("Setting Geometry")
            self.geometry = Geometry(space, time)


        logger.info("Initial Validations Passed, Observation Payload Created")


class KlabNifiListenHTTPClient:
    '''
    Class to submit, an Observation to the Nifi ListenHTTP Processor
    Create an Observation, with :class:`NifiKlabObservation`, and use the `submit` 
    method to submit the created observation query to the ListemHTTP Processor Endpoint
    '''

    def __init__(self,
                 host:str="http://127.0.0.1",
                 port:str="3306",
                 healthport:str=None):
        
        self.host = host
        self.port = port
        self.healthport = healthport

        if self.healthport :
            self.healthCheck()
        else:
            logger.info("Health Check Port not configured, skipping healthcheck...")


    def healthCheck(self):
        resp = requests.get(self.host + ":" + self.healthport + NIFI_HEALTHCHECK_PATH)
        if resp.status_code != 200:
            raise KlabNifiException("HealthCheck failure") 
        logger.info("HealthCheck for ListenHTTP Processor successful")


    def submitObservation(self, obs:KlabObservationNifiRequest):
        logger.debug("Making a Post Request to the Nifi Listen HTTP Endpoint")

        if not obs:
            raise KlabNifiException("Observation cannot be Null")

        try:
            resp = requests.post(
                url=self.host + ":" + self.port,
                json = obs.to_dict()
            )
            
            if resp.status_code != 200:
                raise KlabNifiException(f"Error in Submitting Request, Request failed with Status Code: {resp.status_code}")

        except Exception:
            raise KlabNifiException("Submit Request to the Nifi Endpoint Failed")
        
        logger.info("Submitted an Observation to Nifi Endpoint Successfully")
