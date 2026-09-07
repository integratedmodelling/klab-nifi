from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.documentation import use_case
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship


import pystac
import pystac_client
import tempfile
import requests
import rasterio
from rasterio.vrt import WarpedVRT
from rasterio.merge import merge
from rasterio.enums import Resampling
import os
import sys
from typing import List
import time
from argparse import ArgumentParser
from scipy.ndimage import sum as ndsum
from os.path import isfile
import rasterio as rt
import pandas as pd
import numpy as np
import itertools
from sys import exit


EUNIS_ML_STAC_COLLECTION_ID = "EU_modelV2-1-MECE" ## <- Change this to v3 once  made available by Manu
GET_ML_STAC_COLLECTION_ID = "IUCNGET-V2-MECE"
VITO_STAC_CATALOG = "https://catalogue.weed.apex.esa.int"
IM_STAC_CATALOG = "https://stac.integratedmodelling.org"
RB_STAC_COLLECTION_ID = "global_ecosystem_typology"
HYBRID_STAC_API_COLLECTION_ID = "hybrid_modelling"
CDSE_OIDC_ENDPOINT = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
RDM_BASE_URL = "https://weed-api.iiasa.ac.at"
RDM_COLLECTION_ID = "global_validation"


@use_case("Triggers Hybrid Modelling Pipeline combining outputs from Rule Based Models and Machine Learning Models")
class HybridModellingStarter(FlowFileTransform):


    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Triggers the Hybrid Modelling Pipeline based on the Request'
        tags = ["k.LAB", "WEED", "AI", "ESA", "Digital Twin", "OpenEO", "VITO"]

    SUCCESS = Relationship(name="success", description="Hybrid Models Generated and Pushed to k.LAB STAC Successfully")
    FAILURE = Relationship(name="failure", description="Failed Processing the FlowFile or Failure in generating the Hybrid Outputs or Failure in pushing the data to k.LAB STAC")


    def __init__(self, **kwargs):

        self.bbox = PropertyDescriptor(
            name = "Bounding Box",
            description = "Bounding Box of the Hybrid Request (minX, minY, maxX, maxY)",
            validators = [StandardValidators.NON_EMPTY_VALIDATOR],
            required=True
        )

        self.eo_type = PropertyDescriptor(
            name = "EO Type",
            description = "EO Type: IUCN GET or EUNIS",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True
        )

        self.oidc_client_id = PropertyDescriptor(
            name="OIDC CLIENT ID",
            description="OIDC Client ID for CDSE Backend required to Login and Make Calls to RDM",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True,
            sensitive=True # Senstive Field
        )

        self.oidc_client_secret = PropertyDescriptor(
            name="OIDC CLIENT SECRET",
            description="OIDC Client Secret for CDSE Backend required to Login and Make Calls to RDM",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=True,
            sensitive=True # Sensitive Field
        )

        self.descriptors = [self.bbox, self.eo_type, self.oidc_client_id, self.oidc_client_secret]

    def getPropertyDescriptors(self):
        return self.descriptors

    def generate_hybrid_maps(map1=None,map2=None,matrix1=None,matrix2=None,output=None,name="combined"):
        """
        # description
        #----------------------------------------------------------------------------#
        Combination of classified

        #----------------------------------------------------------------------------#
        Created on Thu Aug 27 10:56:17 2026
        @author: remelgado
        """

        # setup
        #----------------------------------------------------------------------------#


        if map1 is None:

            parser = ArgumentParser(description = 'combine and update classified maps')

            parser.add_argument("-m1", "--map1", help="classified map 1",
                                required=True, type=str)
            parser.add_argument("-m2", "--map2", help="classified map 2",
                                required=True, type=str)

            parser.add_argument("-c1", "--matrix1", required=True, type=str,
                                help="confusion matrix of map 1 (col: reference,column,frequency")
            parser.add_argument("-c2", "--matrix2", required=True, type=str,
                                help="confusion matrix of map 2 (col: reference,column,frequency")

            parser.add_argument("-o", "--output", help="path to output folder",
                                required=True, type=str)

            parser.add_argument("-n", "--name", help="prefix of output filenames",
                                required=False, type=str, default="combined")

            options = parser.parse_args()
            map1 = options.map1
            map2 = options.map2
            matrix1 = options.matrix1
            matrix2 = options.matrix2
            output = options.output
            name = options.name

        # check input paths
        #----------------------------------------------------------------------------#

        if not isfile(map1):
            exit("path to map 1 invalid")

        if not isfile(map2):
            exit("path to map 2 invalid")

        if not isfile(matrix1):
            exit("path to confusion matrix of map 1 invalid")

        if not isfile(matrix2):
            exit("path to confusion matrix of map 2 invalid")

        # access inputs and check data structure
        #----------------------------------------------------------------------------#

        # read confusion matrices
        cm1 = pd.read_table(matrix1, sep=None, engine="python", index_col=0)
        cm2 = pd.read_table(matrix2, sep=None, engine="python", index_col=0)

        # exit if the inputs are not matching
        if cm1.shape != cm1.shape:
            exit("confusion matrices have different numbers of rows/columns")

        # check column names of cm1
        if not all(np.isin(cm1.columns,["reference","predicted","frequency"])):
            exit("confusion matrix for map 1 lacks needed columns")

        # check column names of cm2
        if not all(np.isin(cm2.columns,["reference","predicted	","frequency"])):
            exit("confusion matrix for map 1 lacks needed columns")

        # access map 1
        try:
            m1_ds = rt.open(m1)
        except:
            exit("map 1 is not a valid raster")

        # access map 2
        try:
            m2_ds = rt.open(m2)
        except:
            exit("map 2 is not a valid raster")

        # read rasters
        m1 = m1_ds.read(1)
        m2 = m2_ds.read(1)

        # assign NA value if needed
        m1[np.where(m1 == m1_ds.nodata)] = np.nan
        m2[np.where(m2 == m2_ds.nodata)] = np.nan

        # infer posssible class combinations
        #----------------------------------------------------------------------------#

        # mapped class identifiers
        uc = np.unique(np.concat([m1,m2]))
        uc = uc[~np.isnan(uc)]

        # mean proportion of pixels per class across the target maps
        ma = ((ndsum(m1 > 0, m1, uc) + ndsum(m2 > 0, m2, uc)) / 2) / m1.size
        ma = pd.DataFrame({"prior":ma})
        ma.index = [cm1.index[int(i-1)] for i in uc]

        # define potential combinations of classes
        comb = pd.DataFrame(itertools.product(cm1.index,cm1.index), columns=["A","B"])
        comb["A_id"] = 0
        comb["B_id"] = 0

        # add grid ID for each map
        for x in range(0,cm1.shape[0]):
            comb.loc[comb['A'] == cm1.index[x],'A_id'] = x+1
            comb.loc[comb['B'] == cm1.index[x],'B_id'] = x+1

        # estimate likely class per combination
        #----------------------------------------------------------------------------#

        s = []
        for x in cm1.index:
            p = ma.loc[x].values
            a = np.array(cm1.loc[x,comb["A"].values]).flatten()
            b = np.array(cm2.loc[x,comb["B"].values]).flatten()
            s += [p * a * b]

        # compile results
        scores = pd.DataFrame(s).T
        scores.columns = cm1.index

        # normalize by row
        scores = scores.div(scores.sum(axis=1), axis=0)

        del p, a, b, s

        # reclassify
        #----------------------------------------------------------------------------#

        # output classified map
        oa = np.zeros((m1_ds.height,m1_ds.width), dtype="int32")

        # output stack of class confidences
        ca = np.zeros((m1_ds.height,m1_ds.width,len(cm1.index)), dtype="float32")

        for x in range(0,comb.shape[0]):

            # target pixels
            i = np.where((m1 == comb["A_id"].values[x]) &
                        (m1 == comb["A_id"].values[x]))
            if len(i[0]) == 0:
                next

            # identify most likely class
            oa[i] = np.where(cm1.index == scores.iloc[x].idxmax())[0][0]

            # record class confidences
            ca[i[0],i[1],:] = list(scores.iloc[x])

            del i

        # recover pixels classified in map 1 but not 2
        i = np.where(~np.isnan(m1) & np.isnan(m2))
        if len(i[0] > 0):
            oa[i] = m1[i]

        # recover pixels classified in map 2 but not 1
        i = np.where(np.isnan(m1) & ~np.isnan(m2))
        if len(i[0] > 0):
            oa[i] = m2[i]

        # define outputs and export
        #----------------------------------------------------------------------------#

        # metadata profile
        p = m1_ds.profile.copy()

        # export classified map
        oname = f'{output}/{name}-hybridMap_classification.tif'
        ods = rt.open(oname, "w", **p)
        ods.write(oa, index=1)
        ods.close()

        # export confidence map
        oname = f'{output}/{name}-hybridMap_confidence.tif'
        p.update(count=len(cm1.index)) # update band count
        ods = rt.open(oname, "w", **p)
        ods.write(oa)
        ods.close()

    def make_confusion_matrix_request(client_id:str, client_secret:str, asset_hrefs: List[str], eo_type:str, collection_id:str=RDM_COLLECTION_ID, output_file_path:str="file.xlsx"):

        TOKEN_URL = (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        )

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )

        response.raise_for_status()

        access_token = response.json()["access_token"]

        print(access_token)

        url = f"{RDM_BASE_URL}/userdatasets/confusionmatrix"
        payload = {
            "collectionId": collection_id,
            "stacTifUrls": asset_hrefs,
            "eoType": eo_type
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        response = requests.put(url, json=payload, headers=headers)
        self.logger.info(response.json())
        response.raise_for_status()  # raises an exception for 4xx/5xx responses
        reqdID = response.json().get("id")
        self.logger.info("Polling the Status of the Request")
        status_url = f"{RDM_BASE_URL}/userdatasets/confusionmatrix/{reqdID}"
        excelResultUrl = None

        while 1:
            job = requests.get(status_url, headers=headers)
            job.raise_for_status()
            job_details = job.json()
            if job_details.get("status") == "completed":
                self.logger.info("Confusion Matrix Generation Request handled successfully")
                excelResultUrl = job_details.get("excelResultUrl")
                break
            else:
                self.logger.info("Request still processing. Waiting for 10 seconds before checking again...")
                time.sleep(10)

        with requests.get(excelResultUrl, stream=True) as response:
            response.raise_for_status()

            with open(output_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        print(f"Downloaded to {output_file_path}")

    def download_tiff(href: str, dest_dir: str) -> str:
        """Download a TIFF href to a local temp path."""
        local_path = os.path.join(dest_dir, os.path.basename(href.split("?")[0]))
        with requests.get(href, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        return local_path


    def merge_tiffs(hrefs: list[str], output_path: str, resampling: Resampling = Resampling.nearest):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Download all files first
            local_paths = [download_tiff(href, tmp_dir) for href in hrefs]

            # Determine target CRS from the first asset
            with rasterio.open(local_paths[0]) as first_ds:
                target_crs = first_ds.crs
            print(f"Target CRS (from first asset): {target_crs}")

            # Open all datasets, wrapping any mismatched-CRS ones in a WarpedVRT
            datasets = []
            for path in local_paths:
                ds = rasterio.open(path)
                if ds.crs != target_crs:
                    vrt = WarpedVRT(ds, crs=target_crs, resampling=resampling)
                    datasets.append(vrt)
                else:
                    datasets.append(ds)

            try:
                # Merge into a single mosaic array + transform
                mosaic, out_transform = merge(datasets)

                out_meta = datasets[0].meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": mosaic.shape[1],
                    "width": mosaic.shape[2],
                    "transform": out_transform,
                    "crs": target_crs,
                    "compress": "deflate",
                    "tiled": True,
                    "bigtiff": "IF_SAFER",
                })

                with rasterio.open(output_path, "w", **out_meta) as dest:
                    dest.write(mosaic)

            finally:
                for ds in datasets:
                    ds.close()

        self.logger.info(f"Merged output written to {output_path}")

    def transform(self, context, flowfile):

        if flowfile is None:
            self.logger.error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        dt_url = flowfile.getAttribute("dt.url")
        if dt_url is None or dt_url.strip() == "":
            self.logger.error("DT URL is missing")
            return FlowFileTransformResult(relationship="failure")

        eo_type = context.getProperty(self.eo_type).getValue()
        if eo_type not in ["GET","EUNIS"]:
            self.logger.error("eo_type should be one of EUNIS or GET")
            return FlowFileTransformResult(relationship="failure")

        bbox = context.getProperty(self.bbox).getValue().split(",")
        bbox = [int(item) for item in bbox]

        vito_stac = pystac_client.Client.open(VITO_STAC_CATALOG)
        im_stac = pystac_client.Client.open(IM_STAC_CATALOG)

        match eo_type:
            case "EUNIS":
                ml_items =vito_stac.search(
                            collections=[EUNIS_ML_STAC_COLLECTION_ID],
                            bbox=bbox,
                            limit = 1000).items()
            case "GET":
                ml_items =vito_stac.search(
                            collections=[GET_ML_STAC_COLLECTION_ID],
                            bbox=bbox,
                            limit = 1000).items()
            case _:
                raise ValueError("Invalid eo_type. Must be 'EUNIS' or 'GET'.")

        ml_item_hrefs = [asset.href for item in ml_items for asset in item.assets.values()]


        '''
        Rule Based Outputs are guaranteed to be in 4326
        '''
        rb_items = im_stac.search(
            collections=[RB_STAC_COLLECTION_ID],
            bbox=req_bbox,
            limit = 1000).items()


        match req_eo_type:
            case "EUNIS":
                rb_item_hrefs = [asset.href for item in rb_items for asset in item.assets.values() if "eunis" in asset.extra_fields.get("klab.observable.semantics", "").lower()]
            case "GET":
                rb_item_hrefs = [asset.href for item in rb_items for asset in item.assets.values() if "iucn" in asset.extra_fields.get("klab.observable.semantics", "").lower()]



        ml_item_hrefs = ["https://s3.waw3-1.cloudferro.com/swift/v1/" + asset_href[5:] if "waw3-1" in item else "https://s3.waw4-1.cloudferro.com/swift/v1/" + item[5:] for item in ml_item_hrefs]

        self.logger.info("Generating ML Inferences from " + ml_item_hrefs.join(","))
        self.logger.info("Generating RB Inferences from " + rb_item_hrefs.join(","))

        ml_map = merge(ml_item_hrefs, "ml_map.tif")
        rb_map = merge(rb_item_hrefs, "rb_map.tif")

        make_confusion_matrix_request(ml_item_hrefs , req_eo_type)
        make_confusion_matrix_request(rb_item_hrefs , req_eo_type)

        generate_hybrid_maps(
            map1="ml_map.tif",
            map2="rb_map.tif",
            matrix1="ml_confusion_matrix.xlsx",
            matrix2="rb_confusion_matrix.xlsx",
            output=".",
            name="hybrid"
        )



