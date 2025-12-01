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

ctx = contextualizer.WCS(
    wcsIdentifier="im-data-global-geography__elevation-global-90m",
)

stacCtx = contextualizer.STAC(
    stacCollection="im-data-global-geography", 
    stacAsset="elevation-global-90m"
)



klabNifiObs = KlabObservationNifiRequest(
    ##space = space, 
    ##time = time,
    observationSemantics= "geography:Elevation",
    ##asContext=True,
    ##observationName="el_capital",
    dtURL="https://services.integratedmodelling.org/runtime/main/dt/ESA_INSTITUTIONAL.8ml2b8ft32",
    contextualizer=ctx
)

print (klabNifiObs.to_dict())

print (klabNifiObs.to_json())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)

