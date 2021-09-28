import pandas as pd
import geopandas as gpd


def process_poi_shapefile(file, col, epsg):
    df_poi = gpd.read_file(file)
    df_poi = df_poi.to_crs(epsg)
    df_poi = df_poi[col]
    return df_poi


def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_commer")


def execute(context):
    epsg = context.config("epsg")
    shape_file_commer = context.config("data_path") + context.config("shape_file_commer")

    df_commercial_poi = process_poi_shapefile(shape_file_commer, ["ID", "DRUH_TXT", "geometry"], epsg)
    print(df_commercial_poi)

    
