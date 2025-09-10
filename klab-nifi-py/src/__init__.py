from .geometry import *
from .observation import Observation
from .logging import logger

class NifiKlabObservation:

    def __init__(self, 
                 space:Space=None,
                 time:Time=None,
                 observation:Observation=None):
        
        logger.debug("KLAB Nifi Observation Initialized")
        logger.info("Building the Nifi Observation")

        logger.debug("Setting Geometry")
        self.geometry = Geometry(space, time)

        logger.debug("Validating Observation")
        self.observation = observation

        logger.info("Initial Validations Passed, Observation Payload Created")

