class Semantics:
    def __init__(self, urn):
        self.urn = urn
        self.metadata = Metadata()
        self.type = []
        self.isAbstract = False
        self.collective = False
        self.annotations:list = []
        self.notifications:list = []
        self.namespace:str = "" 
        

class Metadata:
    def __init__(self):
        pass