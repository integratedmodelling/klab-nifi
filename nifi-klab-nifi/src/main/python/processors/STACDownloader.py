"""STAC asset discovery/downloading (does not upload or invoke RDM)."""
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit
import json
import traceback
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship


class STACDownloader(FlowFileTransform):
    class Java:
        #tells NiFi's Java runtime which Java interface this python class fulfills
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'STAC asset discovery/downloading.'
        tags = ['STAC','download','raster']
        dependencies = ['pystac-client','shapely','pyproj','requests']

    SUCCESS = Relationship(name="success", description="Successfully discovered and downloaded corresponding STAC assets")
    FAILURE = Relationship(name="failure", description="Failed to discover or download STAC assets")

    def __init__(self, **kwargs):
        self.bbox = PropertyDescriptor(
            name = "Bounding Box",
            description = "Bounding box as minX, minY, maxX, maxY, a GeoJSON object, or a path to a GeoJSON file",
            validators = [StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.year = PropertyDescriptor(
            name="Reference Year",
            description="Calendar year (YYYY) used to select both ML and RB STAC items. "
                        "May be provided as a FlowFile attribute named year.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.collections = PropertyDescriptor(
            name="STAC Collections",
            description="STAC Collections to query for items. ",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.catalog = PropertyDescriptor(
            name="STAC Catalog URL",
            description="URL of the STAC catalog to query for items.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.asset_key = PropertyDescriptor(
            name="Asset Key",
            description="Optional asset key to select a specific asset from each STAC item. "
                        "If not provided, the processor will attempt to select the first raster asset from each item.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            required=False
        )

        self.descriptors = [self.bbox, self.year, self.collections, self.catalog, self.asset_key]

    def getPropertyDescriptors(self):
        return self.descriptors

    def getRelationships(self):
        return [self.SUCCESS, self.FAILURE]

    @staticmethod
    def context_wgs84(context_input):
        from math import isfinite
        from shapely.geometry import box, shape, mapping
        from shapely.ops import transform, unary_union
        from pyproj import CRS,Transformer

        value = str(context_input).strip()
        if value.startswith("{"):
            obj = json.loads(value)
        else:
            #we divide the attribute by commas to know if its a bbox or a path to a geojson file
            parts = [part.strip() for part in value.split(",")]
            if len(parts) == 4:
                try:
                    min_x, min_y, max_x, max_y = (float(part) for part in parts)
                except ValueError:
                    obj = json.loads(Path(value).read_text(encoding='utf-8'))
                else:
                    if not all(isfinite(coord) for coord in (min_x, min_y, max_x, max_y)):
                        raise ValueError("Bounding box coordinates must be finite numbers")
                    if not (-180 <= min_x < max_x <= 180 and -90 <= min_y < max_y <= 90):
                        raise ValueError(
                            "Bounding box must be minX, minY, maxX, maxY in EPSG:4326 "
                            "with valid, increasing longitude and latitude bounds"
                        )
                    geom = box(min_x, min_y, max_x, max_y)
                    return geom, mapping(geom)
            else:
                obj = json.loads(Path(value).read_text(encoding='utf-8'))

        if obj.get('type') == 'FeatureCollection':
            geom=unary_union([shape(f['geometry']) for f in obj['features']])
        elif obj.get('type') == 'Feature':
            geom=shape(obj['geometry'])
        else:
            geom=shape(obj)
        raw_crs=obj.get('crs')
        source='EPSG:4326'
        if isinstance(raw_crs,dict):
            source=raw_crs.get('properties',{}).get('name',source)
        if source != 'EPSG:4326':
            tx=Transformer.from_crs(CRS.from_user_input(source),'EPSG:4326',always_xy=True)
            geom=transform(tx.transform,geom)
        if not geom.is_valid or geom.is_empty:
            raise ValueError('Invalid context polygon')
        if not (-180 <= geom.bounds[0] <= 180 and -90 <= geom.bounds[1] <= 90 and -180 <= geom.bounds[2] <= 180 and -90 <= geom.bounds[3] <= 90):
            raise ValueError('Context coordinates are not EPSG:4326; declare input CRS in GeoJSON')
        return geom, mapping(geom)

    @staticmethod
    def query_items(catalog_url, collections, context_input, year=None, min_overlap_ratio=1e-8):
        from pystac_client import Client
        from shapely.geometry import shape
        from shapely.ops import transform
        from pyproj import Transformer
        geom,query=STACDownloader.context_wgs84(context_input)
        dt=f'{year}-01-01T00:00:00Z/{year}-12-31T23:59:59Z' if year else None
        client=Client.open(catalog_url)
        #libechange: changed the querying method to GET
        #libechange: added bbox as parameter instead of intersects
        search=client.search(
            collections=collections,
            bbox=geom.bounds,
            datetime=dt,
            max_items=None,
            method="GET"
        )
        tx=Transformer.from_crs('EPSG:4326','EPSG:6933',always_xy=True)
        reference=transform(tx.transform,geom)
        result=[]
        for item in search.items():
            if not item.geometry:
                continue
            footprint=transform(tx.transform,shape(item.geometry))
            ratio=footprint.intersection(reference).area/footprint.area if footprint.area>0 else 0
            if ratio<=min_overlap_ratio:
                continue
            if year is not None:
                instant=(item.properties or {}).get('datetime') or (item.properties or {}).get('start_datetime')
                if instant and str(instant)[:4] != str(year):
                    continue
            result.append(item)
        return result

    @staticmethod
    def choose_raster_assets(items, asset_key=None):
        assets=[]
        for item in items:
            choices=[(key,asset) for key,asset in item.assets.items() if (key==asset_key if asset_key else (asset.href.lower().split('?')[0].endswith(('.tif','.tiff'))))]
            if len(choices)!=1:
                raise ValueError(f'STAC Item {item.id} has {len(choices)} selected raster assets: specify an unambiguous --asset-key')
            key,asset=choices[0]
            assets.append({'collection':item.collection_id, 'item_id':item.id,'asset_key':key,'href':asset.href})
        return assets

    @staticmethod
    def download_assets(assets,target_dir):
        import requests
        target=Path(target_dir)
        if not assets:
            raise ValueError('No raster assets were selected for download')
        paths=[]
        for asset in assets:
            href=asset['href']
            parsed=urlsplit(href)
            #libechange: if the asset is an S3 link, we add the necessary prefix to download it with https
            if parsed.scheme == 's3':
                object_path = f"{parsed.netloc}{parsed.path}"
                href = urlunsplit((
                    'https',
                    's3.waw4-1.cloudferro.com',
                    f"/swift/v1/{quote(object_path, safe='/')}",
                    parsed.query,
                    parsed.fragment
                ))
            elif parsed.scheme not in ('http','https'):
                raise ValueError(f'Only HTTP(S) asset links supported: {href}')

            collection = str(asset.get('collection', 'unknown')).strip()
            collection_path = Path(collection)
            collection_dir = target / collection_path
            collection_dir.mkdir(parents=True,exist_ok=True)

            item_id = str(asset['item_id'])
            filename = f'{quote(item_id, safe="-_.")}.tif'
            path=collection_dir/filename
            with requests.get(href,stream=True,timeout=120) as response:
                response.raise_for_status()
                with path.open('wb') as stream:
                    for block in response.iter_content(8*1024*1024):
                        if block:
                            stream.write(block)
            for existing in collection_dir.glob("*.tif"):
                prefix, separator, previous_item_id = existing.stem.partition("_")
                if separator and len(prefix) >= 4 and prefix.isdigit() and previous_item_id == item_id:
                    existing.unlink()
            paths.append(path)
        return paths

    def transform(self, context, flowfile):

        self.logger.info("========== STAC DOWNLOADING ==========")
        if flowfile is None:
            self.logger.error("Incoming flowfile is null")
            return FlowFileTransformResult(relationship="failure")

        #first we check if the processor has any set properties
        bbox = context.getProperty(self.bbox).getValue()
        if bbox is None or not bbox.strip():
            #if not, we check if the flowfile has the defined attribute
            bbox = flowfile.getAttribute("bbox")
        if bbox is None or not bbox.strip():
            self.logger.error("Bounding Box is missing from the processor property and FlowFile attribute 'bbox'")
            return FlowFileTransformResult(relationship="failure")

        year = context.getProperty(self.year).getValue()
        if year is None or not year.strip():
            year = flowfile.getAttribute("year")
        if year is None or not year.strip():
            self.logger.error("Year is missing from the processor property and FlowFile attribute 'year'")
            return FlowFileTransformResult(relationship="failure")

        collections = context.getProperty(self.collections).getValue()
        if collections is None or not collections.strip():
            collections = flowfile.getAttribute("collections")
        if collections is None or not collections.strip():
            self.logger.error("STAC Collections is missing from the processor property and FlowFile attribute 'collections'")
            return FlowFileTransformResult(relationship="failure")
        collections = [collection.strip() for collection in collections.split(",") if collection.strip()]
        if not collections:
            self.logger.error("STAC Collections must contain at least one collection ID")
            return FlowFileTransformResult(relationship="failure")

        catalog = context.getProperty(self.catalog).getValue()
        if catalog is None or not catalog.strip():
            catalog = flowfile.getAttribute("catalog")
        if catalog is None or not catalog.strip():
            self.logger.error("STAC Catalog URL is missing from the processor property and FlowFile attribute 'catalog'")
            return FlowFileTransformResult(relationship="failure")

        asset_key = context.getProperty(self.asset_key).getValue()
        if asset_key is None or not asset_key.strip():
            asset_key = flowfile.getAttribute("asset_key")
        if asset_key is not None and not asset_key.strip():
            asset_key = None

        try:
            year = int(str(year).strip())
            if len(str(year).strip()) != 4 or not (1900 <= year <= 2100):
                raise ValueError("year must be four digits between 1900 and 2100")
        except(TypeError, ValueError):
            self.logger.error("Year attribute is invalid: must be a four-digit integer between 1900 and 2100")
            return FlowFileTransformResult(relationship="failure")
        year_datetime = f"{year}-01-01T00:00:00Z/{year}-12-31T23:59:59Z"
        self.logger.info(f"STAC Downloader: Using year {year_datetime} for STAC item selection.")

        try:
            #query stac items
            items = self.query_items(catalog, collections, bbox, year)

            if not items:
                self.logger.error("No STAC items found for the given parameters.")
                return FlowFileTransformResult(relationship="failure")

            #we make sure bbox is the right format for context_wgs84
            print(f"STAC Downloader: Using bounding box {bbox} for STAC item selection.")

            _, context_geojson = self.context_wgs84(bbox)

            #choose raster assets
            assets = self.choose_raster_assets(items, asset_key)

            #download assets
            target_dir = Path("/opt/nifi/output/stac_download")
            downloaded_paths = self.download_assets(assets, target_dir)

            rb_collection, ml_collection = collections

            ml_raster_paths = [
                str(path)
                for asset, path in zip(assets, downloaded_paths)
                if asset["collection"] == ml_collection
            ]

            rb_raster_paths = [
                str(path)
                for asset, path in zip(assets, downloaded_paths)
                if asset["collection"] == rb_collection
            ]

            if not ml_raster_paths or not rb_raster_paths:
                raise ValueError("No downloaded rasters found for the ML or RB collection")

            return FlowFileTransformResult(
                relationship="success",
                attributes={"ml_raster_path": json.dumps(ml_raster_paths),
                            "rb_raster_path": json.dumps(rb_raster_paths),
                            "fusion_context_geojson": json.dumps(context_geojson)
                            }
            )

        except Exception as e:
            self.logger.error(f"An error occurred while processing STAC items: {e}")
            traceback.print_exc()
            return FlowFileTransformResult(
                relationship="failure",
                attributes={"error": str(e)}
            )