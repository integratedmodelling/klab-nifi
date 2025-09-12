from .commons import *
from shapely import wkt
from shapely.errors import WKTReadingError
from shapely.geometry import Point, LineString, Polygon
from typing import List, Union
from .logging import logger
from datetime import datetime, timezone
from .exception import *

class Space(BaseModel):
    def __init__(self, shape:Union[List[tuple[float, float]] , str], grid:str="1.km"):

        if not shape:
            raise KlabNifiException("Shape cannot be None")

        if isinstance(shape, str):
            try:
                geom = wkt.loads(shape)
                logger.info("WKT String Validated Successfully")

            except WKTReadingError:
                raise KlabNifiException("Invalid Geometry")
        else:
            try:
                if len(shape) == 1:
                    geom = Point(shape[0])  
                elif len(shape) == 2:
                    geom = LineString(shape)
                else:
                    geom = Polygon(shape)
                
                if not geom.is_valid:
                    raise KlabNifiException("Invalid Geometry")
                
                logger.info("Geometry Validated Successfully")
            
            except Exception:
                raise KlabNifiException("Invalid Geomtry")
            
        
        self.shape = KLAB_GEO_PROJ + " " + geom.wkt
        self.sgrid = grid
        self.proj = KLAB_GEO_PROJ

class Time(BaseModel):

    TIME_SCALES = ["year"] ## Add to the scales here

    def __init__(self,
                 tstart:Union[datetime, int]=None, 
                 tend: Union[datetime, int]=None,
                 tscope: int=1,
                 tunit:str="year"):
        
        if isinstance(tstart, str):
            if not self.validate(tstart):
                raise KlabNifiException("Starting Timestamp is wrong")
        
        if isinstance(tend, str):
            if not self.validate(tend):
                raise KlabNifiException("End Timestamp is wrong")
            
        if tunit.lower() not in self.TIME_SCALES:
            raise KlabNifiException("Time Unit is wrong")


        self.tstart = tstart
        self.tend = tend
        self.tunit = tunit
        self.tscope = tscope

    @staticmethod
    def validate(timestamp_str:str)->bool:
        if not timestamp_str.isdigit():
            return False
        
        try:
            ts_ms = int(timestamp_str)
            # Convert milliseconds to seconds
            ts_sec = ts_ms / 1000

            # Try converting to datetime (will raise if out of range)
            datetime.utcfromtimestamp(ts_sec)
            return True
        except (ValueError, OverflowError):
            return False


class Geometry(BaseModel):
    '''
    Creates a Geometry, with Space and Time
    '''

    def __init__(self, space:Space=None, time:Time=None):

        if not space or not time:
            raise KlabNifiException("In geometry, both Spatial and Temporal Dimensions are required")
        
        self.space = space
        self.time = time


