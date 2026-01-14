from pydantic.dataclasses import dataclass
from dataclasses import field
from .commons import BaseModel


@dataclass
class Contextualizer(BaseModel):
    '''
    Base Contextualizer Class to be inherited all the Contextualizers
    :param serviceType: [Internal] Service Type of the Contextualizer, internally set, not to be set by user
    '''
    serviceType: str = field(init=False, default=None)
    persistent: bool = field(init=False, default=False)

    def __setattr__(self, name, value):
        if name == "serviceType" and hasattr(self, "serviceType"):
            raise AttributeError("serviceType is read-only and set internally")
        super().__setattr__(name, value)


@dataclass
class WCS(Contextualizer):

    '''
    WCS (Web Coverage Service) contextualization

    :param wcsIdentifier: [Required] WCS Coverage Identifier
    :param band: [Optional, Default 0] Band Number (For Multi Band Coverages. Handling Single Band by Default)
    :param wcsVersion: [Optional, Default "2.0.1"] WCS Version
    :param serviceUrl: [Optional, Default "https://integratedmodelling.org/geoserver/ows"] WCS Service URL
    :param persistent: [Optional, Default False] Whether the Contextualization is to be Persistent Resource
    '''
    
    wcsIdentifier: str
    band: int = 0 ## Handling Single Band by Default
    wcsVersion: str = "2.0.1" ## Default WCS Version
    serviceUrl: str = "https://integratedmodelling.org/geoserver/ows" ## Referring to the IM GeoServer by default

    def __post_init__(self):
        object.__setattr__(self, "serviceType", "wcs")



@dataclass
class STAC(Contextualizer):
    '''
    STAC (SpatioTemporal Asset Catalog) contextualization

    :param collection: [Required] STAC Collection URL
    :param asset: [Required] STAC Asset Name
    :param band: [Optional, Default 0] Band Number (For Multi Band CoGs. This is usually unusual)
    :param persistent: [Optional, Default False] Whether the Contextualization is to be Persistent Resource
    '''

    collection: str
    asset: str
    band: int = 0

    def __post_init__(self):
        object.__setattr__(self, "serviceType", "stac")

