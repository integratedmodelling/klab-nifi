from klab_nifi_py import *


space = Space(
    shape= "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))"
)

dt_2020 = datetime(2020, 1, 1, 0, 0, 0)
dt_2021 = datetime(2021, 12, 31, 23, 59, 59)


time = Time(
    tstart=dt_2020,
    tend = dt_2021
    )



klabNifiObs = KlabObservationNifiRequest(
    space = space, 
    time = time,
    observationName= "AM1729",
    observationSemantics= "earth:Terrestrial earth:Region",
    #dtURL="https://services.integratedmodelling.org/runtime/main/dt/ESA_INSTITUTIONAL.40ipl26qekk"
    #id = -1,
)

print (klabNifiObs.to_dict())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)
