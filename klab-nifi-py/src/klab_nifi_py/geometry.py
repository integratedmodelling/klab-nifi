from .commons import *
from shapely import wkt
from shapely.errors import WKTReadingError
from shapely.geometry import Point, LineString, Polygon
from typing import List, Union
from .logging import logger
from datetime import datetime, timezone
from .exception import *

class Space(BaseModel):
    def __init__(self, 
                 shape:Union[List[tuple[float, float]] , str], 
                 grid:str="1.km",
                 proj:str=KLAB_GEO_PROJ):

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
            
        
        self.shape = proj + " " + geom.wkt
        self.sgrid = grid
        self.proj = proj

class Time(BaseModel):

    TIME_SCALES = ["year"] ## Add to the scales here

    def __init__(self,
                 tstart:Union[datetime, int]=None, 
                 tend: Union[datetime, int]=None,
                 tscope: int=1,
                 tunit:str="year"):
        
        if isinstance(tstart, int):
            if not self.validate(tstart):
                raise KlabNifiException("Starting Timestamp is wrong")
        elif isinstance(tstart, datetime):
            tstart = int(tstart.timestamp() * 1000)
        else:
            raise KlabNifiException("Start Timestamp should either be a int or a datettime object")
        
        if isinstance(tend, int):
            if not self.validate(tend):
                raise KlabNifiException("End Timestamp is wrong")
        elif isinstance(tend, datetime):
            tend = int(tend.timestamp() * 1000)
        else:
            raise KlabNifiException("End Timestamp should either be a int or a datettime object")
        
        if tunit.lower() not in self.TIME_SCALES:
            raise KlabNifiException("Time Unit is wrong")
        
        if tstart > tend:
            raise KlabNifiException("Start Time cannot be greater than End Time")


        self.tstart = tstart
        self.tend = tend
        self.tunit = tunit
        self.tscope = tscope

    @staticmethod
    def validate(timestamp_str:int)->bool:
        '''
        validate the int timestamp
        '''
        try:
            ts_ms = int(timestamp_str)
            ts_sec = ts_ms / 1000
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


