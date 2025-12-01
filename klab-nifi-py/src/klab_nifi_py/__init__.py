from .geometry import *
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
                 asContext:bool=False,
                 space:Space=None,
                 time:Time=None,
                 dtURL:str=None,
                 id:int=None,
                 contextualizer:Contextualizer=None,
                 loglevel:str=logging.INFO):
        
        logger.debug("KLAB Nifi Observation Initialized")
        logger.info("Building the Nifi Observation")
        logger.setLevel(loglevel)

        if not observationName and asContext:
            raise KlabNifiException("Observation Name cannot be non null for Context Observations")
        else:
            if observationName:
                logger.info("Setting Name to the Observation")
                self.name = observationName
        
        if not observationSemantics:
            raise KlabNifiException("Observation Query must be made with a Semantics")

        logger.info("Setting Name and Semantics to the Observation")

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

        if dtURL :
            logger.debug("Setting the Digital Twin URL")
            self.digitalTwin = dtURL
        else:
            raise KlabNifiException("Digital Twin URL cannot be Null for Observation Request")

        if id:
            logger.debug("Setting observation ID")
            self.id = id
        else:
            logger.debug("Setting unresolved observation ID")
            self.id = -1

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
