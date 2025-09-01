class Geometry:
    def __init__(self, shape):
        self.shape = shape


class Params:
    def __init__(self, shape, sgrid, proj:str="EPSG:4326"):
        self.proj = proj
        self.shape = shape
        self.sgrid = sgrid

    def verifyShape(self)->bool:
        return True
    
    def verifyGrid(self)-> bool:
        return True
    
    def verifyProj(self)->bool:
        return True
    


class Dimension:
    def __init__(self, dimType, regular: bool = True, 
                 dimensionality: int = 2):
        self.type = dimType
        self.regular = regular
        self.dimensionality = dimensionality
        self.parameters:list = []