from .geometry import *
from .observation import Observation
from .logging import logger
import requests
import logging


class NifiKlabObservation(BaseModel):

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
    To submit a k.LAB Observation to the Nifi Server
    With the Standard ListenHTTP Processor in Apache Nifi
    '''
    def __init__(self,
                 host:str="127.0.0.1",
                 port:str="3306",
                 healthport:str=None,
                 obs:NifiKlabObservation=None):
        
        if not obs:
            raise KlabNifiException("Observation not initialized")
        

        self.host = host
        self.port = port
        self.observation = obs
        self.healthport = healthport

        if self.healthport :
            self.healthCheck()


    def healthCheck(self):
        resp = requests.get(self.host + ":" + self.healthport)
        if resp.status_code != 200:
            raise KlabNifiException("HealthCheck failure") 
        logger.info("HealthCheck for ListenHTTP Processor successful")


    def submit(self):
        logger.debug("Making a Post Request to the Nifi Listen HTTP Endpoint")

        try:
            resp = requests.post(
                url=self.host + ":" + self.port,
                json = self.observation.to_dict()
            )
            
            if resp.status_code != 200:
                raise KlabNifiException(f"Error in Submitting Request, Request failed with Status Code: {resp.status_code}")

        except Exception:
            raise KlabNifiException("Submit Request to the Nifi Endpoint Failed")
        
        logger.info("Submitted an Observation to Nifi Endpoint Successfully")
