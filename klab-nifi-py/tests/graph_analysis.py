import os
import osmnx as ox
import geopandas as gpd
import rasterio
from shapely.geometry import LineString, Point
import numpy as np
import pyrosm as osm
from shapely.geometry import LineString, MultiLineString

from collections import Counter

pbf_file = r'C:\Users\arnab.moitra\Desktop\valhalla\bilbao.osm.pbf'  
dem_raster = r'C:\Users\arnab.moitra\Desktop\valhalla\bilbao_dem.tiff'
max_slope = 0.05  # 5% slope threshold
osm = osm.OSM(pbf_file)
network_type = "walking" 

edges = osm.get_network(network_type=network_type)

print ("Initial Edge Count:", len(edges))
#print (edges.count())

#edges.to_file("edges.gpkg", driver="GPKG")

# ---------------------------
# STEP 3: Remove stairs
# ---------------------------
edges = edges[edges['highway'] != 'steps']

print ("Edge Count after removing stairs:", len(edges))

print(edges.iloc[0]["geometry"])
print ("+++++")

print (edges.columns)

for item in edges.columns:
    print (item)
    c = Counter(edges[item])
    print (c.most_common(5))
    print ("++++++")
    


#print (roads.head())

# ---------------------------
# STEP 4: Sample DEM to compute slope
# ---------------------------
# Open DEM
dem = rasterio.open(dem_raster)

def compute_slope(geom):
    lines = []
    if isinstance(geom, LineString):
        lines = [geom]  # already a single line
    elif isinstance(geom, MultiLineString):
        # Flatten all LineStrings inside
        lines = list(geom.geoms)
    else:
        raise TypeError("Input geometry must be LineString or MultiLineString")
    
    maxSlope = -999999999999999
    for line in lines:
        start = line.coords[0]
        end = line.coords[-1]
        start_elev = list(dem.sample([start]))[0][0]
        end_elev = list(dem.sample([end]))[0][0]
        if not (start_elev is None or end_elev is None):
            if line.length <= 0:
                print (lines)
                continue
            slope = abs(end_elev - start_elev) / line.length
            maxSlope = max(maxSlope, slope)


            
    return maxSlope

edges['slope'] = edges['geometry'].apply(compute_slope)

# ---------------------------
# STEP 5: Filter by slope
# ---------------------------
edges_flat = edges[edges['slope'] <= max_slope]

print ("Edge Count after filtering by slope:", len(edges_flat))
#edges_flat.to_file("edges_flat.gpkg", driver="GPKG")

ids_to_keep = list(edges_flat['id'])
with open("output.txt", "w", encoding="utf-8") as f:
    for item in ids_to_keep:
        f.write(str(item) + "\n")

# Optional: convert GPKG -> OSM PBF using ogr2ogr + osmconvert (requires osmconvert installed)
#os.system("ogr2ogr -f 'OSM' roads_flat.osm roads_flat.gpkg lines")
#os.system("osmconvert roads_flat.osm -o=roads_flat.osm.pbf")

        

    
