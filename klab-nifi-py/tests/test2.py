import logging
import os

## klab Imports
from klab.klab import Klab
from klab.geometry import GeometryBuilder
from klab.observable import Observable
from klab.utils import Export, ExportFormat

import asyncio
import os
from shapely import wkt
import geopandas as gpd

LOGGER = logging.getLogger(__name__)
STANDARD_PATH = os.path.join(os.path.expanduser('~'), ".klab", "testcredentials.properties")

async def ARIES_request(klab: Klab, area_WKT: str, obs_res: str, obs_year: int, observable: str,
                        export_format: ExportFormat, export_path: str):
    
    # create the semantic type and geometry/time to init the CONTEXT
    obs = Observable.create("earth:Region")
    grid = GeometryBuilder().grid(urn=area_WKT, resolution=obs_res).years(obs_year).build()

    # submit to engine to generate the CONTEXT
    ticketHandler = klab.submit(obs, grid)
    context = await ticketHandler.get()

    dataflow = context.getDataflow(ExportFormat.KDL_CODE)
    provenenace = context.getProvenance(True, ExportFormat.ELK_GRAPH_JSON)


    # define the observable (dataset or model) and submit to context
    obsData = Observable.create(observable)
    ticketHandler = context.submit(obsData)

    data = await ticketHandler.get()

    # retrieve the dataset and export to disk
    data.exportToFile(Export.DATA, export_format, export_path)

    dataflow = context.getDataflow(ExportFormat.KDL_CODE)
    provenenace = context.getProvenance(True, ExportFormat.ELK_GRAPH_JSON)

    print (dataflow)
    print ("===========================")
    print (provenenace)

def get_klab_instance(fpath: str = STANDARD_PATH) -> Klab:
    try:
        print('- try Remote Engine connection ....')
        klab = Klab.create(credentialsFile=fpath)
        ##raise RuntimeError('Skipping Remote Engine connection for testing purposes')
    except:
        try:
            print('- try Local Engine connection ...')
            klab = Klab.create()
        except:
            raise RuntimeError('Could not establish connection to a k.lab engine')

    if klab and klab.isOnline():
        print(f'* connection to {klab.engine.url} was successfully established. session: {klab.engine.session}')
    else:
        raise EnvironmentError('could not establish connection to the klab instance')

    return klab

if __name__ == "__main__":
    LOGGER.info("Starting k.LAB Plugin Model")
    ##klab_engine_url = args.get('klab_engine_url', 'http://localhost:8080')
    klab_certificate_path = r'C:\Users\arnab.moitra\.klab\klab.prod.cert'
    year = 2020
    semantic_query = "type of aries.colombia.ecosystem:ModelledColombianEcosystem"  # Example semantic query
    spatial_context_wkt = "EPSG:4326 POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))"

    LOGGER.info(f" Querying k.LAB Semantic Web with Query: {semantic_query}")

    try:
        klab = get_klab_instance(klab_certificate_path)
        asyncio.run(ARIES_request(
            klab=klab,
            area_WKT=spatial_context_wkt,
            obs_res="1 km",
            obs_year=year,
            observable=semantic_query,
            export_format=ExportFormat.BYTESTREAM,
            export_path=os.path.join(r'C:\Users\arnab.moitra\Desktop\klab-nifi\klab-nifi-py\tests', "result.tif")
        ))

    except Exception as e:
        LOGGER.error(f"An error occurred while executing the k.LAB model: {e}")
        raise e
    
    finally:
        if klab:
            klab.close()

    LOGGER.info('Done!')

