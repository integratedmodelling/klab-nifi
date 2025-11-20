from klab_nifi_py import *


space = Space(
    shape= "POLYGON ((77.11183309467745 28.558406270223728, 77.13158011521391 28.57649245320077, 77.1527376372173 28.55914959926227, 77.13355481726757 28.54056479885392, 77.11183309467745 28.558406270223728))",
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
    observationName="dt_test",
    observationSemantics= "earth:Terrestrial earth:Region",
    dtURL="https://services.integratedmodelling.org/integration/runtime/main/dt/ESA_INSTITUTIONAL.kh4au4ha8c"
    #id = -1,
)

print (klabNifiObs.to_dict())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)
