import requests
from dataclasses import dataclass
from typing import Union, List


@dataclass
class Timings:
    period: str
    opening_time: str
    closing_time: str


class ClimateShelterType:
    '''
    Enum for different types of climate shelters.
    '''

    MUNICIPAL_BUILDING = "Municipal Building"
    LIBRARY = "Library"
    DISTRICT_MUNICIPAL_CENTER = "District Municipal Center"
    SPORTS_CENTER = "Sports Center"
    MUNICIPAL_MARKET = "Municipal Market"
    CULTURAL_CENTER = "Cultural Center"
    MUSEUM = "Museum"
    TRANSPORT_STATION = "Transport Station"
    CHURCH = "Church"
    EXHIBITION_HALL = "Exhibition Hall"
    SHOPPING_CENTER = "Shopping Center"

@dataclass
class ClimateShelterItem:
    '''
    These would be coming under properties of the STAC item. 
    '''
    id: str
    name: str
    type: ClimateShelterType
    centroid: tuple(float, float) # type: ignore
    timings: List[Timings]
    

