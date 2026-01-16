from klab_nifi_py import * 


space = Space(
    shape= "POLYGON ((4.785959977018684 52.36327282792627, 4.888466900960927 52.39865946645364, 4.919564507100708 52.37241513986268, 4.8996006118010955 52.3367728809811, 4.785959977018684 52.36327282792627))"
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
    ##persistent=True,
    collection="https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a",
    asset="red",
    
)



klabNifiObs = KlabObservationNifiRequest(
    space=space,
    time = time,
    ##resetContext=True,
    ##observationSemantics= "geography:Elevation",
    observationSemantics= "earth:Terrestrial earth:Region",
    asContext=True,
    observationName="am1729",
    dtURL="https://services.integratedmodelling.org/integration/runtime/main/dt/ESA_INSTITUTIONAL.i0iqi1uedz",
    ##contextualizer=stacCtx,
)

print (klabNifiObs.to_dict())

print (klabNifiObs.to_json())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)

