from klab_nifi_py import *


space = Space(
    shape= "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))"
)

time = Time(
    tstart=1325376000000,
    tend = 1356998400000
    )



klabNifiObs = KlabObservationNifiRequest(
    space = space, 
    time = time,
    observationName= "AM1729",
    observationSemantics= "earth:Terrestrial earth:Region"
)

print (klabNifiObs.to_dict())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)
