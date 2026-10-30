from klab_nifi_py import * 


space = Space(
    grid="10.m",
    shape= "POLYGON ((25.33 36.81,25.84 36.81,25.84 37.20,25.33 37.20,25.33 36.81))"
)

dt_2020 = datetime(2024, 1, 1, 0, 0, 0)
dt_2019 = datetime(2019, 12, 31, 23, 59, 59)

time = Time(
    tstart=dt_2019,
    tend = dt_2020
    )


klabContext = Context(
    space=space,
    time=time,
    ctxObservationNamespace="nifi.internal.tests",
    ctxObservationName="AM1729-DT_SLOW_FLOW"
)



persistantResourceConfig = contextualizer.PersistentResource(
    mode = contextualizer.PersistentResource.ResourceUpdateMode.UPDATE,
    namespace="klab.nifi.vito.ml.inference.resources",
    #namespace = "klab.nifi.internal.tests.resources",
    #service="im.resources-main",
    catalog ="staging",
    id="eunis"
    #id="machine_learning_inference_eunis",
)



ctx = contextualizer.WCS(
    wcsIdentifier="im-data-global-geography__elevation-global-90m",
    #wcsIdentifier="im-data-global-geography__slope-global-90m",
    resource=persistantResourceConfig
)

vitoStac = contextualizer.STAC(
    collection="https://catalogue.weed.apex.esa.int/collections/EUNIS2021plus-V320-MECE",
    asset="openEO",
    band = 0,
    s3EndpointUrl = "https://s3.waw4-1.cloudferro.com",
    #resource=persistantResourceConfig
)

msftStac = contextualizer.STAC(
    collection="https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02",
    asset="data",
    band = 0,
    #resource=persistantResourceConfig
)

#im.resources-main:staging:klab.nifi.internal.tests.resources:slope


#im.resources-main:staging:klab.nifi.vito.ml.resources:ml_inference_eunis

klabNifiObs = KlabObservationNifiRequest(
    ctx= klabContext,
    observationSemantics="geography:Elevation",
    dtURL="https://services.integratedmodelling.org/runtime/main/api/v1/dt/ESA_INSTITUTIONAL.1e47cawocjpI",
    contextualizer= vitoStac
)

print (klabNifiObs.to_dict())

print (klabNifiObs.to_json())

nifiklabClient = KlabNifiListenHTTPClient(port="3306", healthport="3307")
nifiklabClient.submitObservation(klabNifiObs)

