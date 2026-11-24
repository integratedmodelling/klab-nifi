# TODO 1: import the libraries needed for the script here
from typing import List
from pathlib import Path
import json
from osgeo import gdal
import os
#necessary imports for NiFi processor
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.relationship import Relationship
from nifiapi.properties import PropertyDescriptor, StandardValidators


class MergeRasters(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = "Merges downloaded rasters for each STAC collection."
        tags = ["merge", "raster", "mosaic"]
        dependencies = ["GDAL==3.6.2"]

    SUCCESS = Relationship(name='success', description='The input rasters have been correctly merged into a mosaic raster.')
    FAILURE = Relationship(name='failure', description='The input rasters could not be merged.')

    def __init__(self, *args, **kwargs):
        super().__init__()

    
        self.raster_folder_paths = PropertyDescriptor(
            name='Input folder Paths',
            description='Paths to the folers containing the rasters to be merged',
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        ) 

        self.descriptors = [self.raster_folder_paths]

    def getPropertyDescriptors(self) -> List[PropertyDescriptor]:
        return self.descriptors

    def getRelationships(self):
        return [self.SUCCESS, self.FAILURE]

    @staticmethod
    def merge_rasters_in_folder(folder, output_dir=None, output_suffix="_merged.tif", dst_nodata=0, compress="DEFLATE", tiled=True, overwrite=True ):
        """
        TAKEN FROM ARIES TOOLBOX
        Merges all .tif/.tiff raster files inside a single folder into one GeoTIFF.

        Parameters:
        - folder: str, path to the folder containing raster files to merge.
        - output_dir: str, optional, path to the directory where the merged GeoTIFF will be saved. Default is the input folder. 
        - output_suffix: str, optional, suffix added to the output file name. Default is "_merged_total.tif".
        - dst_nodata: int or float, optional, nodata value assigned to the output. Default is 0.
        - compress: str, optional, compression method for the output GeoTIFF. Default is "DEFLATE".
        - tiled: bool, optional, whether to tile the output GeoTIFF. Default is True.
        - overwrite: bool, optional, whether to overwrite the existing merged file. Default is True.

        Returns:
        - output_file: str, path to the saved merged GeoTIFF.
        """

        # Gather all TIFF files in the folder
        file_list = [
            os.path.join(folder, f).replace("\\", "/")
            for f in os.listdir(folder)
            if f.lower().endswith((".tif", ".tiff"))
        ]

        if not file_list:
            print(f"No TIFF files found in {folder}. Skipping.")
            return None

        # Define output name and location
        folder_name = os.path.basename(folder.rstrip("/\\"))
        output_name = f"{folder_name}{output_suffix}"
        output_path = os.path.join(output_dir or folder, output_name).replace("\\", "/")

        # Skip if file exists and overwrite=False
        if os.path.exists(output_path) and not overwrite:
            print(f"File already exists: {output_path} (skipping)")
            return output_path

        print(f"Merging {len(file_list)} rasters in {folder}...")

        # Merge rasters
        gdal.Warp(
            output_path,
            file_list,
            format="GTiff",
            dstNodata=dst_nodata,
            creationOptions=[
                f"COMPRESS={compress}",
                f"TILED={'YES' if tiled else 'NO'}"
            ]
        )

        print(f"Merged: {output_path}")
        return output_path


    def transform(self, context, flowfile) -> FlowFileTransformResult:
        """
        Runs once per incoming FlowFile.
        """


        self.logger.info("================= STARTING RASTER MERGING =================")

        if flowfile is None:
            self.logger.error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        raster_folder_paths = context.getProperty(self.raster_folder_paths).getValue()
        if raster_folder_paths is None or not raster_folder_paths.strip():
            raster_folder_paths = flowfile.getAttribute("downloaded_stac_paths")
        if raster_folder_paths is None or not raster_folder_paths.strip():
            self.logger.error(" raster_folder_paths missing from the processor property and FlowFile attribute 'downloaded_stac_paths'")
            return FlowFileTransformResult(relationship="failure")    
        
        try:

            #libe: ein moduren baten hartzen deuzela attribute horretako folder danak ta ein mergea folder danatan
            # bebai pentseu zelan eingogun ahal izeteko erabili processor desberdinen ostean, ze euki behar deu izen konkretu bet flowfilean

            merged_rasters = []

            #flowfile attribute needs to be of the following format:
            # {"collections": {"collection1": {"folder": "folder", "rasters": "raster1", "raster2"}}, {"collection2"...}}
            data = json.loads(raster_folder_paths.strip())
            collections = data["collections"]

            for colelction, details in collections.items():
                #details contains the folder and each raster path
                folder_path = Path(details["folder"])
                merged_raster_path = folder_path.parent
                merged_raster = MergeRasters.merge_rasters_in_folder(str(folder_path), str(merged_raster_path))
                if merged_raster is not None:
                    merged_rasters.append(merged_raster)

        
            return FlowFileTransformResult(
                relationship="success",
                attributes={
                    "merged_rasters": str(merged_rasters)
                },
            )

        except Exception as e:
            import traceback
            # TODO 13: Add the description of the error when the processor fails
            self.logger.error(f"An error occurred when xxxx : {e}")
            traceback.print_exc()
            return FlowFileTransformResult(
                relationship="failure",
                attributes={"error": str(e)},
            )
        
