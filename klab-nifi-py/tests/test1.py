from klab_nifi_py import * 


space = Space(
    shape= "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))",
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
    stacCollection="https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a", 
    stacAsset="red"
)



klabNifiObs = KlabObservationNifiRequest(
    ##space = space, 
    ##time = time,
    observationSemantics= "geography:Slope",
    ##observationSemantics= "earth:Terrestrial earth:Region",
    ##asContext=True,
    ##observationName="el_capital",
    dtURL="https://services.integratedmodelling.org/runtime/main/dt/ESA_INSTITUTIONAL.f8zx3uma7o",
    contextualizer=stacCtx
)

print (klabNifiObs.to_dict())

print (klabNifiObs.to_json())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)

