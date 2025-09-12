from .logging import logger
from .exception import *
from .commons import BaseModel

class Observation(BaseModel):
    def __init__(self, name:str, semantics:str):

        if not name :
            raise KlabNifiException("Observation Name cannot be non null")
        
        if not semantics:
            raise KlabNifiException("Observation Query must be made with a Semantics")

        logger.info("Setting Name and Semantics to the Observation")
        self.name = name

        ## To check how can we validate the semantics here without the Python Client
        ## Keeping it as it is for now
        self.semantics = semantics 
