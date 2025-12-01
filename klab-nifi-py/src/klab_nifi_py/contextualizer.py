from dataclasses import dataclass, field
from .commons import BaseModel


class Contextualizer(BaseModel):
    pass


@dataclass
class WCS(Contextualizer):
    serviceType: str = field(default="wcs", init=False)
    wcsIdentifier: str
    band: int = 0 ## Handling Single Band by Default
    wcsVersion: str = "2.0.1" ## Default WCS Version
    serviceUrl: str = "https://integratedmodelling.org/geoserver/ows" ## Referring to the IM GeoServer by default

    def __post_init__(self):
        self.serviceType = "wcs"
    


@dataclass
class STAC(Contextualizer):
    serviceType: str = field(default="stac", init=False)
    stacCollection: str
    stacAsset: str

    def __post_init__(self):
        self.serviceType = "stac"

