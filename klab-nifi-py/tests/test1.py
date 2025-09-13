from klab_nifi_py import *


space = Space(
    shape= "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))"
)

time = Time(
    tstart=1325376000000,
    tend = 1356998400000
    )



obs = Observation(
    name = "AM1729",
    semantics="earth:Terrestrial earth:Region"
)

obs.name = "777"


klabNifiObs = NifiKlabObservation(
    space = space, 
    time = time,
    observation=obs
)

print (klabNifiObs.to_dict())

nifiklabClient = Client(port="3306", healthport="3307")
nifiklabClient.submit(klabNifiObs)
