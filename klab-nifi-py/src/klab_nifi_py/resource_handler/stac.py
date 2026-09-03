
import requests
from ..logging import logger
from typing import Tuple


EO_BANDS_KEY = "eo:bands"
FEATURES_KEY = "features"
ASSETS_KEY = "assets"
LINKS_KEY = "links"
STAC_ROOT = "root"

class STAC_Handler:
    '''
    Imports the STAC with appropriate contextualization data to add 
    the required metdata for Data access and processing 
    '''

    @staticmethod
    def STACValidate(collectionURL:str, assetId: str)->Tuple[bool, str, str]:
        '''
        Check if the Asset is present in the STAC Collection
        :param collectionURL: The URL of the STAC Collection
        :param assetId: The ID of the Asset to check
        :return: True if the Asset is present, False otherwise
        :return: The S3 Endpoint URL if the Asset is in S3, None otherwise
        :return: The Collection ID if the Asset is present, None otherwise
        :raises ValueError: If the collectionURL is invalid or the assetId is not found
        '''

        logger.info(f"Checking the Asset Key for URL: {collectionURL}, for asset: {assetId} and Performing S3 Handling if required")
        try:

            catalogURL, collectionID = None, None
            jsonResp = requests.get(collectionURL)
            jsonResp.raise_for_status()
            links = jsonResp.json().get(LINKS_KEY, [])
            collectionID = jsonResp.json().get("id", None)

            logger.info(f"Found the Collection ID: {collectionID}")
    
            for link in links:
                if link.get("rel") == STAC_ROOT:
                    catalogURL = link.get("href")
                    break

            if not catalogURL or collectionID is None:
                raise ValueError("Could not find catalog URL or collection ID in the STAC collection response")

            payload = {
                "collections": [collectionID],
                "bbox": [-180, -90, 180, 90],
                "limit": 1000
            }

            searchURL = None
            catalogJSON = requests.get(catalogURL).json()
            for link in catalogJSON.get(LINKS_KEY, []):
                if link.get("rel") == "search":
                    searchURL = link.get("href")
                    break
            if not searchURL:
                raise ValueError("Could not find search URL in the STAC catalog response")

            response = requests.post(searchURL, json=payload)
            response.raise_for_status()
            collection_data = response.json()

            features = collection_data.get("features", [])
            asset = None

            for feature in features:
                assets = feature.get(ASSETS_KEY, {})
                if assetId in assets.keys():
                    asset = assets[assetId]
                else:
                    for asset_info in assets.values():
                        if EO_BANDS_KEY in asset_info:
                            for band in asset_info[EO_BANDS_KEY]:
                                if band.get("name") == assetId:
                                    asset = asset_info
                                    break
                
                if asset is not None:
                     break
                        
            if asset is not None:
                asset_href = asset.get("href", "")
                if asset_href.startswith("s3://"):
                    logger.info(f"Asset {assetId} found in STAC collection with S3 URL: {asset_href}")
                    if ("waw3" in asset_href):
                            return( True, "https://s3.waw3-1.cloudferro.com", collectionID)
                    elif ("waw4" in asset_href):
                            return( True, "https://s3.waw4-1.cloudferro.com", collectionID)
                else:
                    return (True, None, collectionID)
            
            raise ValueError(f"Asset {assetId} not found in STAC collection {collectionURL}")
                        

        except requests.exceptions.RequestException as e:
            raise ValueError(f"Error fetching STAC collection: {e}")