from .semantics import Semantics, Metadata
from .metadata import Metadata

class Observable:
    def __init__(self, 
                 name:str, 
                 urn:str,
                 optional:bool = False):
        self.name = name
        self.urn = urn
        self.artifctType = "OBJECT"
        self.semantics = Semantics(name)
        self.optional: False = optional
        self.metadata = Metadata()
        