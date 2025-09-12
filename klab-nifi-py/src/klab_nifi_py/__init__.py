from .geometry import *
from .observation import Observation
from .logging import logger
import requests
import logging


class NifiKlabObservation(BaseModel):
    '''
    The Main Observation Class in Python for creating the JSON Payload passing to 
    the Observation Relay Processor through the flowfile. If directly passed, the 
    Nifi Processor should be written in Python in that case, which is possible Nifi 2.0 onwards.

    If using the ListenHTTP Processor in Apache Nifi, use the :class:`Client` class, and use the 
    `submit` method.

    '''

    def __init__(self, 
                 space:Space=None,
                 time:Time=None,
                 observation:Observation=None,
                 loglevel:str=logging.INFO):
        
        logger.debug("KLAB Nifi Observation Initialized")
        logger.info("Building the Nifi Observation")
        logger.setLevel(loglevel)

        if space and time :
            logger.debug("Setting Geometry")
            self.geometry = Geometry(space, time)

        logger.debug("Setting Observation")
        if not observation:
            raise KlabNifiException("Observation can't be null")
        self.observation = observation

        logger.info("Initial Validations Passed, Observation Payload Created")


class Client:
    '''
    Class to submit, an Observation to the Nifi ListenHTTP Processor
    Create an Observation, with :class:`NifiKlabObservation`, and use the `submit` 
    method to submit the created observation query to the ListemHTTP Processor Endpoint
    '''

    def __init__(self,
                 host:str="127.0.0.1",
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
        resp = requests.get(self.host + ":" + self.healthport)
        if resp.status_code != 200:
            raise KlabNifiException("HealthCheck failure") 
        logger.info("HealthCheck for ListenHTTP Processor successful")


    def submit(self, obs:NifiKlabObservation):
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
