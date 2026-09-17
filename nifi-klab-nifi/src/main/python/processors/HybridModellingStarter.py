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
from rasterio.warp import transform_bounds
from rasterio.windows import from_bounds, transform as window_transform
import scipy
from typing import List
import time
from argparse import ArgumentParser
from scipy.ndimage import sum as ndsum
from os.path import isfile
import rasterio as rt
import pandas as pd
import numpy as np
import itertools
import os
import sys
from sys import exit
import xml.etree.ElementTree as ET


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
            required=False
        )

        self.eo_type = PropertyDescriptor(
            name = "EO Type",
            description = "EO Type: IUCN GET or EUNIS",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.typology_level = PropertyDescriptor(
            name = "Typology Level",
            description = "Typology Level 1, 2 or 3, Generic to Specific Heirarchical Ordering",
            allowable_values=["1", "2", "3"],
            required = False,
            default_value = "3" # By default, the most specific one
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

        self.descriptors = [self.bbox, self.eo_type, self.typology_level, self.oidc_client_id, self.oidc_client_secret]

    def getPropertyDescriptors(self):
        return self.descriptors

    def confusion_matrix_preprocessing(self, matrix:str=None, sheet_name:str=None, raster_code_mapping:dict=None)->pd.DataFrame:
        cm = pd.read_excel(matrix, sheet_name=sheet_name, header=None)
        interest = cm.shape[1] - 3

        self.logger.info("Found Interest to be: " + str(interest))

        reference = cm.iloc[2, 2:2 + interest].tolist()

        # Reference classes are in rows 3:9
        predicted = cm.iloc[3:3 + interest, 1].tolist()

        # Actual confusion-matrix values
        counts = cm.iloc[3:3 + interest, 2: 2 + interest].copy()

        # Give it proper labels
        counts.index = reference
        counts.columns = predicted

        # Convert strings -> raster codes
        counts.index = counts.index.map(raster_code_mapping)
        counts.columns = counts.columns.map(raster_code_mapping)

        # Make sure values are numeric
        counts = counts.apply(pd.to_numeric)
        self.logger.info(counts.head(20).to_string())
        return counts


    def generate_hybrid_maps(self, typology_level:int, map1=None,map2=None,matrix1=None,matrix2=None,output=None,name="combined", raster_code_mapping=None):
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


        self.logger.info("Generating Hybrid Maps for Typology Level: " + str(typology_level))
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
            self.logger.error("path to map 1 invalid")
            return False

        if not isfile(map2):
            self.logger.error("path to map 2 invalid")
            return False

        if not isfile(matrix1):
            self.logger.error("path to confusion matrix of map 1 invalid")
            return False

        if not isfile(matrix2):
            self.logger.error("path to confusion matrix of map 2 invalid")
            return False

        # access inputs and check data structure
        #----------------------------------------------------------------------------#

        # read confusion matrices
        sheet_name = "Level"+str(typology_level)+"Results"
        self.logger.info("Fetching sheet " + sheet_name + " from the Confusion Matrix Excel")

        # access map 1
        try:
            m1_ds = rt.open(map1)
        except Exception as e:
            self.logger.error(f"map 1 is not a valid raster, due to {e}")
            return False

        # access map 2
        try:
            m2_ds = rt.open(map2)
        except Exception as e:
            self.logger.error(f"map 2 is not a valid raster, dur to {e}")
            return False

        # read rasters
        m1 = m1_ds.read(1).astype(np.float32)
        m2 = m2_ds.read(1).astype(np.float32)

        # assign NA value if needed
        m1[np.where(m1 == m1_ds.nodata)] = np.nan
        m2[np.where(m2 == m2_ds.nodata)] = np.nan

        # infer posssible class combinations
        #----------------------------------------------------------------------------#

        # mapped class identifiers
        uc = np.unique(np.concat([m1,m2]))
        uc = uc[~np.isnan(uc)]  ## <- Here the classes like 100101, 100102 so on and so forth

        cm1 = self.confusion_matrix_preprocessing(matrix1, sheet_name=sheet_name, raster_code_mapping=raster_code_mapping)
        cm2 = self.confusion_matrix_preprocessing(matrix2, sheet_name=sheet_name, raster_code_mapping=raster_code_mapping)

        classes = cm1.index.union(cm2.index).union(pd.Index(uc.astype(int)))
        cm1 = cm1.reindex(index=classes,columns=classes,fill_value=0)
        cm2 = cm2.reindex(index=classes,columns=classes,fill_value=0)


        # mean proportion of pixels per class across the target maps
        ma = ((ndsum(m1 > 0, m1, uc) + ndsum(m2 > 0, m2, uc)) / 2) / m1.size  ## For each class, average number of pixels
        ma = pd.DataFrame({"prior":ma})
        ma.index = [int(i) for i in uc] ## prottek ta classes er corresponding probability ta store kore akta dataframe banalam
        ma = ma.reindex(classes, fill_value=0)
        self.logger.info(ma.head().to_string())

        cm1_prob = cm1.div(cm1.sum(axis=1).replace(0, np.nan), axis=0).fillna(0)
        cm2_prob = cm2.div(cm2.sum(axis=1).replace(0, np.nan), axis=0).fillna(0)

        comb = pd.DataFrame(itertools.product(uc, uc), columns=["A", "B"])

        self.logger.info(comb.head().to_string())


        # estimate likely class per combination
        #----------------------------------------------------------------------------#

        ### cm1: confusion matrix 1, cm2: confusion matrix 2, ma: another dataframe with all the classes as index and the value as the probabilities of classes
        scores = []
        for x in ma.index:
            p = ma.loc[x, "prior"]
            a = cm1_prob.loc[x, comb["A"].values].to_numpy()
            b = cm2_prob.loc[x, comb["B"].values].to_numpy()
            scores.append(p * a * b)

        # compile results
        scores = pd.DataFrame(scores).T
        scores.columns = cm1.index

        # normalize by row
        row_sums = scores.sum(axis=1)
        scores = scores.div(row_sums.replace(0, np.nan), axis=0).fillna(0)

        # reclassify
        #----------------------------------------------------------------------------#

        # output classified map
        height, width = m1.shape
        oa = np.zeros((height, width), dtype="int32")
        ca = np.zeros((len(classes), height, width), dtype="float32")

        for row_idx in range(comb.shape[0]):
            A_val = comb["A"].iloc[row_idx]
            B_val = comb["B"].iloc[row_idx]

            row_scores = scores.iloc[row_idx]
            if row_scores.sum() == 0:
                continue  # this (A,B) combo never appeared in the confusion matrices -> no evidence, skip

            # pixels where map1 == A AND map2 == B (fixed: was m1==... & m1==... in the original)
            mask = (m1 == A_val) & (m2 == B_val)
            if not mask.any():
                continue

            best_class = row_scores.idxmax()  # actual class code, not positional index
            oa[mask] = best_class
            ca[:, mask] = row_scores.to_numpy()[:, np.newaxis]

        # --- Recover pixels valid in only one map ---
        only_m1 = ~np.isnan(m1) & np.isnan(m2)
        oa[only_m1] = m1[only_m1]

        only_m2 = np.isnan(m1) & ~np.isnan(m2)
        oa[only_m2] = m2[only_m2]

        # define outputs and export
        #----------------------------------------------------------------------------#

        # metadata profile
        p = m1_ds.profile.copy()

        # export classified map
        self.logger.info("Writing the Final Classes")
        oname = f'{output}/hybridMap_classification.tif'
        ods = rt.open(oname, "w", **p)
        ods.write(oa, indexes=1)
        ods.close()

        # export confidence map
        self.logger.info("Writing Confidence Map")
        oname = f'{output}/hybridMap_confidence.tif'
        p.update(count=len(cm1.index)) # update band count
        ods = rt.open(oname, "w", **p)
        ods.write(ca)
        ods.close()

        self.logger.info("Successfully generated Hybrid Map and Confidence Maps")
        return True

    def make_confusion_matrix_request(self,
                                        client_id:str,
                                        client_secret:str,
                                        asset_hrefs: List[str],
                                        eo_type:str,
                                        collection_id:str=RDM_COLLECTION_ID,
                                        output_excel_file_path:str="file.xlsx",
                                        output_parquet_file_path:str="file.parquet",
                                        fetch_mapping_from_csv:bool=True):

        response = requests.post(
            CDSE_OIDC_ENDPOINT,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )

        response.raise_for_status()
        access_token = response.json()["access_token"]
        self.logger.info("Successfully Retrieved the Access Token from CDSE Endpoint")
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
        self.logger.info(str(payload))
        self.logger.info(str(response.json()))
        response.raise_for_status()  # raises an exception for 4xx/5xx responses
        reqdID = response.json().get("id")
        self.logger.info("Polling the Status of the Request")
        status_url = f"{RDM_BASE_URL}/userdatasets/confusionmatrix/{reqdID}"
        excelResultUrl = None
        parquetResultUrl = None
        qmlResultUrl = None

        while 1:
            job = requests.get(status_url, headers=headers)
            job.raise_for_status()
            job_details = job.json()
            if job_details.get("status") == "completed":
                self.logger.info("Confusion Matrix Generation Request handled successfully")
                self.logger.info(str(job_details))
                excelResultUrl = job_details.get("excelResultUrl")
                parquetResultUrl = job_details.get("parquetResultUrl")
                qmlResultUrl = job_details.get("qmlUrl")
                break
            else:
                self.logger.info("Request still processing. Waiting for 10 seconds before checking again...")
                time.sleep(10)

        if excelResultUrl is None or parquetResultUrl is None:
            self.logger.error("Couldn't find the Excel Result or the Parquet Result in the RDM Response")
            return False

        with requests.get(excelResultUrl, stream=True) as response:
            response.raise_for_status()

            with open(output_excel_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        self.logger.info(f"Downloaded Confusion Matrix (.xlsx) to {output_excel_file_path}")

        with requests.get(parquetResultUrl, stream=True) as response:
            response.raise_for_status()

            with open(output_parquet_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        self.logger.info(f"Downloaded Parquet file (.parquet) to {output_parquet_file_path}")

        mapping = None
        if fetch_mapping_from_csv:
            with requests.get(qmlResultUrl, stream=True) as response:
                response.raise_for_status()

                with tempfile.TemporaryFile() as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                    f.seek(0)

                    self.logger.info(f"Downloaded CSV Raster Mappings from {qmlResultUrl}, Starting to parse the CSV file to get the raster code mappings")
                    df = pd.read_csv(f)
                    self.logger.info("EO Type provided: " + eo_type)

                    eo_type = eo_type.strip().upper()
                    valid_types = df["EOTYPE"].str.upper().unique().tolist()
                    if eo_type not in valid_types:
                        raise ValueError(f"eo_type must be one of {valid_types}, got '{eo_type}'")

                    filtered = df[df["EOTYPE"].str.upper() == eo_type]

                    # raster_value comes in as float (e.g. 10101.0), so cast for a clean int key
                    mapping = dict(zip(filtered["name"], filtered["raster_value"].astype(int)))

        return True, mapping

    def download_tiff(self, href: str, dest_dir: str) -> str:
        """Download a TIFF href to a local temp path."""
        local_path = os.path.join(dest_dir, os.path.basename(href.split("?")[0]))
        with requests.get(href, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        return local_path


    def merge_tiffs(self, hrefs: list[str], output_path: str, bbox: List[float] = None,
                     nodata_value: float = None, resampling: Resampling = Resampling.nearest,
                     target_resolution: float = 0.01):

        self.logger.info("Proceeding to Merging the Tiffs")
        target_crs = rasterio.crs.CRS.from_epsg(4326)
        res = (target_resolution, target_resolution)

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Download all files first
            local_paths = [self.download_tiff(href, tmp_dir) for href in hrefs]
            if nodata_value is None:
                with rasterio.open(local_paths[0]) as first_ds:
                    nodata_value = first_ds.nodata

            self.logger.info(f"Target CRS: {target_crs}, target resolution: {res} deg")

            out_bounds = None
            if bbox is not None:
                # bbox is already in EPSG:4326, so no reprojection needed here
                out_bounds = tuple(bbox)
                self.logger.info(f"Forcing output bounds to bbox: {out_bounds}")

            # Open all datasets. Always wrap in a WarpedVRT so both CRS
            # reprojection (if needed) and resampling to the target
            # resolution are enforced uniformly, even for inputs that are
            # already in EPSG:4326 but at a different pixel size.
            datasets = []
            for path in local_paths:
                ds = rasterio.open(path)
                needs_reproject = ds.crs != target_crs
                needs_resample = (
                    round(abs(ds.transform.a), 8) != target_resolution
                    or round(abs(ds.transform.e), 8) != target_resolution
                )

                if needs_reproject or needs_resample:
                    vrt = WarpedVRT(
                        ds,
                        crs=target_crs,
                        resampling=resampling,
                        nodata=nodata_value,
                        resolution=res,
                    )
                    datasets.append(vrt)
                    self.logger.info(
                        f"{path}: reproject={needs_reproject}, resample={needs_resample} "
                        f"(orig res=({ds.transform.a}, {ds.transform.e}))"
                    )
                else:
                    datasets.append(ds)

            try:
                # Merge into a single mosaic array + transform, forcing both
                # the pixel resolution and (optionally) output bounds so
                # uncovered areas are filled with nodata rather than cropped away
                mosaic, out_transform = merge(
                    datasets,
                    bounds=out_bounds,
                    res=res,
                    nodata=nodata_value,
                    resampling=resampling,
                )

                # merge() snaps bounds to the pixel grid, which can leave
                # extra rows/cols beyond the exact bbox. Crop back to bbox.
                if out_bounds is not None:
                    self.logger.info("Rounding off per the bbox bounds")
                    window = from_bounds(*out_bounds, transform=out_transform)
                    # floor the offset, ceil the size, so we never crop into
                    # bbox-covered data — only trim what's truly outside it
                    window = window.round_offsets(op="floor").round_lengths(op="ceil")

                    col_off, row_off = int(window.col_off), int(window.row_off)
                    width, height = int(window.width), int(window.height)

                    # clamp to array bounds just in case rounding overshoots
                    col_off = max(col_off, 0)
                    row_off = max(row_off, 0)
                    width = min(width, mosaic.shape[2] - col_off)
                    height = min(height, mosaic.shape[1] - row_off)

                    cropped_window = rasterio.windows.Window(col_off, row_off, width, height)
                    mosaic = mosaic[:, row_off:row_off + height, col_off:col_off + width]
                    out_transform = window_transform(cropped_window, out_transform)

                    self.logger.info(
                        f"Cropped mosaic to exact bbox: shape={mosaic.shape}, "
                        f"transform={out_transform}"
                    )

                out_meta = datasets[0].meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": mosaic.shape[1],
                    "width": mosaic.shape[2],
                    "transform": out_transform,
                    "crs": target_crs,
                    "nodata": nodata_value,
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

        dt_url = flowfile.getAttribute("dt_url")
        if dt_url is None or dt_url.strip() == "":
            self.logger.error("DT URL is missing")
            return FlowFileTransformResult(relationship="failure")

        eo_type = context.getProperty(self.eo_type).getValue()
        if eo_type == None:
            eo_type = flowfile.getAttribute("eo_type")

        if eo_type.upper() not in ["GET","EUNIS"]:
            self.logger.error("eo_type should be one of EUNIS or GET")
            return FlowFileTransformResult(relationship="failure")

        bbox = context.getProperty(self.bbox).getValue()
        if bbox == None:
            bbox = flowfile.getAttribute("bbox")

        if bbox == None:
            self.logger.error("BBOX shouldn't be null")
            return FlowFileTransformResult(relationship="failure")

        bbox = bbox.split(",")
        bbox = [float(item) for item in bbox]

        client_id = context.getProperty(self.oidc_client_id).getValue()
        client_secret = context.getProperty(self.oidc_client_secret).getValue()

        vito_stac = pystac_client.Client.open(VITO_STAC_CATALOG)
        im_stac = pystac_client.Client.open(IM_STAC_CATALOG)

        match eo_type.upper():
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
            bbox=bbox,
            limit = 1000).items()


        match eo_type:
            case "EUNIS":
                rb_item_hrefs = [asset.href for item in rb_items for asset in item.assets.values() if "eunis" in asset.extra_fields.get("klab.observable.semantics", "").lower()]
            case "GET":
                rb_item_hrefs = [asset.href for item in rb_items for asset in item.assets.values() if "iucn" in asset.extra_fields.get("klab.observable.semantics", "").lower()]

        ml_item_hrefs = ["https://s3.waw3-1.cloudferro.com/swift/v1/" + item[5:] if "waw3-1" in item else "https://s3.waw4-1.cloudferro.com/swift/v1/" + item[5:] for item in ml_item_hrefs]

        self.logger.info("Generating ML Inferences from " + ",".join(ml_item_hrefs))
        self.merge_tiffs(ml_item_hrefs, "raster/ml_map.tif", bbox)
        success, raster_code_mapping = self.make_confusion_matrix_request(
                                client_id,
                                client_secret,
                                ml_item_hrefs ,
                                eo_type,
                                RDM_COLLECTION_ID,
                                "excel/ml_confusion_matrix.xlsx",
                                "parquet/ml_parquet.parquet",
                                True)
        if not success:
            self.logger.error("Confusion Matrix wasn't generated successfully for Machine Learning Model Outputs")
            return FlowFileTransformResult(relationship="failure")



        self.logger.info("Generating RB Inferences from " + ",".join(rb_item_hrefs))
        self.merge_tiffs(rb_item_hrefs, "raster/rb_map.tif", bbox)
        success, _ = self.make_confusion_matrix_request(
                        client_id,
                        client_secret,
                        rb_item_hrefs ,
                        eo_type,
                        RDM_COLLECTION_ID,
                        "excel/rb_confusion_matrix.xlsx",
                        "parquet/rb_parquet.parquet",
                        False)

        if not success:
            self.logger.info("Confusion Matrix wasn't generated successfully for Rule Based Model Outputs")
            return FlowFileTransformResult(relationship="failure")


        self.logger.info("Confusion Matrices are created, and the Maps have been generated, generating Hybrid Maps and Confusion Matrices")

        result = self.generate_hybrid_maps(
            typology_level = int(context.getProperty(self.typology_level).getValue()),
            map1="raster/ml_map.tif",
            map2="raster/rb_map.tif",
            matrix1="excel/ml_confusion_matrix.xlsx",
            matrix2="excel/rb_confusion_matrix.xlsx",
            output="raster/",
            name="hybrid",
            raster_code_mapping=raster_code_mapping
        )

        if not result:
            self.logger.error("Error while creating the Hybrid Maps from ML and RB Workflows and confusion matrices")
            return FlowFileTransformResult(relationship="failure")

        self.logger.info("Successfully generated Hybrid Maps and Confidence Maps")
        return FlowFileTransformResult(relationship="success")