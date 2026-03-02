from pydantic.dataclasses import dataclass
from dataclasses import field
from .commons import BaseModel, KLAB_SERVICETYPE_KEY
from .logging import logger


IM_RESOURCES_SERVICE = "im.resources-main"
IM_RESOURCES_WCS_URL = "https://integratedmodelling.org/geoserver/ows"

@dataclass
class PersistentResource(BaseModel):
    '''
    Persistent Resource for Contextualization

    :param service: [Required] The Service which has the Resource first
    :param catalog: [Required] The Catalog where the resource is to be located
    :param id: [Required] Resource ID
    :param namespace: [Optional, Default: "im:nifi"] Namespace for the Persistent Resource
    :param mode: [Optional, Default: UPDATE] Persistent Resource Update Mode
    '''

    class ResourceUpdateMode:
        '''
        Persistent Resource Update Modes
        Modes:
            UPDATE: Update the existing Persistent Resource with new Contextualization
            REPLACE: Replace the existing Persistent Resource with new Contextualization
            MERGE: Merge the new Contextualization with existing Persistent Resource
            ADD: Add new Contextualization as a new Persistent Resource
        '''
        UPDATE = "update"
        REPLACE = "replace"
        MERGE = "merge"
        ADD = "add"

    catalog: str
    id: str
    namespace: str
    service: str = IM_RESOURCES_SERVICE ## The Resource Service that has the resource, Default: im.resources-main
    mode: str = ResourceUpdateMode.UPDATE #Default: Update


@dataclass(kw_only=True)
class Contextualizer(BaseModel):
    '''
    Base Contextualizer Class to be inherited all the Contextualizers
    :param serviceType: [Internal] Service Type of the Contextualizer, internally set, not to be set by user
    '''
    serviceType: str = field(init=False, default=None)
    persistent: bool = field(default=False, kw_only=True)
    resource: PersistentResource = field(default=False, kw_only=True) ## Only if persistent is True

    def __setattr__(self, name, value):
        if name == KLAB_SERVICETYPE_KEY and hasattr(self, KLAB_SERVICETYPE_KEY):
            raise AttributeError("serviceType is read-only and set internally")
        super().__setattr__(name, value)


    def persistantConfigCheck(self):
        '''
        If the Resource Configuration is provided, set the Persistent Flag to True
        even if the User has not set the Persistent Flag to True or has set it to False
        This is to ensure that the Contextualization is always Persistent if the Resource
        '''

        if self.resource is not None:
            logger.info("Setting Persistance basis the Resource Configuration Provided")
            self.persistent = True
        else:
            logger.info("No Resource Configuration Provided, Setting Persistance to False")
            self.persistent = False

@dataclass(kw_only=True)
class WCS(Contextualizer):

    '''
    WCS (Web Coverage Service) contextualization

    :param wcsIdentifier: [Required] WCS Coverage Identifier
    :param band: [Optional, Default: 0] Band Number (For Multi Band Coverages. Handling Single Band by Default)
    :param wcsVersion: [Optional, Default: "2.0.1"] WCS Version
    :param serviceUrl: [Optional, Default: "https://integratedmodelling.org/geoserver/ows"] WCS Service URL
    :param persistent: [Optional, Default: False] Whether the Contextualization is to be Persistent Resource
    '''
    
    wcsIdentifier: str
    band: int = 0 ## Handling Single Band by Default
    wcsVersion: str = "2.0.1" ## Default WCS Version
    serviceUrl: str = IM_RESOURCES_WCS_URL ## Referring to the IM GeoServer by default
    resource: PersistentResource = None ## Only if persistent is True

    def __post_init__(self):
        object.__setattr__(self, KLAB_SERVICETYPE_KEY, "wcs")
        self.persistantConfigCheck()


@dataclass(kw_only=True)
class STAC(Contextualizer):
    '''
    STAC (SpatioTemporal Asset Catalog) contextualization

    :param collection: [Required] STAC Collection URL
    :param asset: [Required] STAC Asset Name
    :param band: [Optional, Default 0] Band Number (For Multi Band CoGs. This is usually unusual)
    :param persistent: [Optional, Default: False] Whether the Contextualization is to be Persistent Resource
    '''

    collection: str
    asset: str
    band: int = 0
    resource: PersistentResource = None

    def __post_init__(self):
        object.__setattr__(self, KLAB_SERVICETYPE_KEY, "stac")
        self.persistantConfigCheck()




