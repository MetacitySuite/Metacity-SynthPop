import pandas as pd
import geopandas as gpd
import json


epsg = "EPSG:25832"

#zones - ZSJ


df_zones = gpd.read_file("./data/shp/SHP_kodZSJ/KodUzemi_Praha.shp")
df_zones = df_zones[["kodUzemi", "geometry"]]
df_zones = df_zones.to_crs(epsg)
df_zones.columns = ["zone_id", "geometry"] #rename columns

df_home = gpd.read_file("./data/shp/OBYVATELSTVO/obyv_adr_body.shp")
df_home = df_home[["Sum_PTOTAL", "geometry"]]
df_home = df_home.to_crs(epsg)

df_home['geometry'] = df_home['geometry'].centroid
df_home = gpd.sjoin(df_home, df_zones, op = "within", how="left").drop(columns=["index_right"])

