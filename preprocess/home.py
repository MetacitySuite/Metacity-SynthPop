import pandas as pd
import geopandas as gpd

def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_home")
    context.stage("preprocess.zones")

def execute(context):
    epsg = context.config("epsg")
    df_zones = context.stage("preprocess.zones")

    df_home = gpd.read_file(context.config("data_path") + context.config("shape_file_home"))
    df_home = df_home[["ID", "Sum_PTOTAL", "geometry"]]
    df_home = df_home.to_crs(epsg)

    #the average number of residents; geometry
    df_home.columns = ["id", "resident_number", "geometry"] #rename columns
    df_home["id"] = df_home["id"].astype(str) #was float
    df_home["id"] = df_home["id"].str.split('.').str[0]

    #add zone_id
    df_home['geometry'] = df_home['geometry'].centroid
    df_home = gpd.sjoin(df_home, df_zones, op = "within", how="left").drop(columns=["index_right"])

    #drop unsjoined houses
    df_home = df_home.dropna(subset=['zone_id'])   
    return df_home