from klab_nifi_py import * 


space = Space(
    shape= "POLYGON ((77.11827805440052 28.583302839837145, 77.20532608330569 28.59787853608833, 77.19317984671426 28.55681256232853, 77.11827805440052 28.583302839837145))"
)

dt_2020 = datetime(2020, 1, 1, 0, 0, 0)
dt_2021 = datetime(2021, 12, 31, 23, 59, 59)


time = Time(
    tstart=dt_2020,
    tend = dt_2021
    )


persistantResourceConfig = contextualizer.PersistentResource(
    namespace="nifi.internal.tests",
    service="im.resources-main",
    catalog ="staging",
    id="dummy_aspect",
)

ctx = contextualizer.WCS(
    wcsIdentifier="im-data-global-geography__elevation-global-90m",
    resource=persistantResourceConfig
)

stacCtx = contextualizer.STAC(
    ##persistent=True,
    collection="https://planetarycomputer.microsoft.com/api/stac/v1/collections/landsat-c2-l2",
    asset="red",
    
)



klabNifiObs = KlabObservationNifiRequest(
    ##space = space,
    ##time  = time,
    ##resetContext=True,
    observationSemantics= "geography:Aspect",
    ##observationSemantics= "earth:Terrestrial earth:Region",
    ##observationNamespace="nifi.internal.tests",
    ##asContext=True,
    ##observationName="delhi",
    dtURL="https://services.integratedmodelling.org/runtime/main/api/v1/dt/ESA_INSTITUTIONAL.8itnxba3hm",
    contextualizer=ctx,
)

print (klabNifiObs.to_dict())

print (klabNifiObs.to_json())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)

