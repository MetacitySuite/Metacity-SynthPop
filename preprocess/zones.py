import pandas as pd
import geopandas as gpd

def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_zones")

def execute(context):
    epsg = context.config("epsg")
    df_zones = gpd.read_file(context.config("data_path") + context.config("shape_file_zones"))
    df_zones = df_zones.to_crs(epsg)
    df_zones = df_zones[["kodUzemi", "geometry"]]
    df_zones.columns = ["zone_id", "geometry"] #rename columns
   
    return df_zones