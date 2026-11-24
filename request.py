import requests


a = {
    "bbox":"-75, 10, -74, 11",  # minX, minY, maxX, maxY (example bounding box for Czech Republic)
    "eo_type":"GET",
    "dt_url":"https://a.b.c.d",
    "year":"2024",
    "catalog":"https://catalogue.weed.apex.esa.int/",
    "collections":"RuleBased-EXTENT-V502,MachineLearning-IUCNGET-EXTENT-V330", #first rulebased collection, then machine learning collection
    "asset_key":"openEO",
    "mapping_csv":r"/opt/nifi/input/eo_maps.csv",
    "parquet_path":r"/opt/nifi/input/global_validation.parquet"
}


resp = requests.post(
    "http://127.0.0.1:3306",json=a
)

print(resp.status_code)
#print (resp.json())