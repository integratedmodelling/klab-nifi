from klab_nifi_py import * 


space = Space(
    shape= "POLYGON(( 88.2660 22.4492, 88.4611 22.4492, 88.4611 22.6823, 88.2660 22.6823, 88.2660 22.4492))"
)

dt_2020 = datetime(2020, 1, 1, 0, 0, 0)
dt_2021 = datetime(2021, 12, 31, 23, 59, 59)


time = Time(
    tstart=dt_2020,
    tend = dt_2021
    )


persistantResourceConfig = contextualizer.PersistentResource(
    mode = contextualizer.PersistentResource.ResourceUpdateMode.UPDATE,
    namespace="klab.nifi.internal.tests.resources",
    service="im.resources-main",
    catalog ="staging",
    id="slope",
)



ctx = contextualizer.WCS(
    #wcsIdentifier="im-data-global-geography__elevation-global-90m",
    wcsIdentifier="im-data-global-geography__slope-global-90m",
    resource=persistantResourceConfig
)

stacCtx = contextualizer.STAC(
    collection="https://planetarycomputer.microsoft.com/api/stac/v1/collections/landsat-c2-l2",
    asset="red",

    
)


#im.resources-main:staging:klab.nifi.internal.tests.resources:slope


klabNifiObs = KlabObservationNifiRequest(
    space = space,
    time  = time,
    ##resetContext=True,
    ##observationSemantics= "geography:Slope",
    observationSemantics= "earth:Terrestrial earth:Region",
    observationNamespace="nifi.internal.tests",
    asContext=True,
    observationName="WB",
    dtURL="https://services.integratedmodelling.org/runtime/main/api/v1/dt/ESA_INSTITUTIONAL.rvr3s2juw0",
    #contextualizer=ctx
)

print (klabNifiObs.to_dict())

print (klabNifiObs.to_json())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)

