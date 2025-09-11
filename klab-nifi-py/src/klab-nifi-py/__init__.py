from .geometry import *
from .observation import Observation
from .logging import logger
import logging

class NifiKlabObservation:

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

