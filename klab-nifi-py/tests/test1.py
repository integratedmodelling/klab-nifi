from klab_nifi_py import *


space = Space(
    shape= "POLYGON ((77.16543215041936 28.592841668452053, 77.2012588876784 28.628008632138815, 77.25231904077988 28.596804731731098, 77.23228991995003 28.551963865365067, 77.16543215041936 28.539821338621195, 77.15104503545706 28.577483387087256, 77.16543215041936 28.592841668452053))"
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
    observationName="demo_nifi",
    observationSemantics= "earth:Terrestrial earth:Region",
    dtURL="https://services.integratedmodelling.org/runtime/main/dt/ESA_INSTITUTIONAL.dklrj2nku8"
    #id = -1,
)

print (klabNifiObs.to_dict())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)
