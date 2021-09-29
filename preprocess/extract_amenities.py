import pandas as pd
import geopandas as gpd
import json

def load_poi_tags(file):
    poi_tags_file = open(file)
    return json.load(poi_tags_file)
     

def process_poi_shapefile(file, col, epsg):
    gdf_poi = gpd.read_file(file)
    gdf_poi = gdf_poi[col]
    gdf_poi = gdf_poi.to_crs(epsg)
    gdf_poi.columns = ["id", "type", "description", "geometry"]
    return gdf_poi


def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("preprocess.zones")
    context.config("shape_file_commercial")
    context.config("shape_file_amenity")
    context.config("shape_file_leisure")
    context.config("poi_tags_work")
    context.config("poi_tags_education")
    context.config("poi_tags_shop")
    context.config("poi_tags_leisure")

def execute(context):
    epsg = context.config("epsg")
    df_zones = context.stage("preprocess.zones")

    shape_file_commercial = context.config("data_path") + context.config("shape_file_commercial")
    shape_file_amenity = context.config("data_path") + context.config("shape_file_amenity")
    shape_file_leisure = context.config("data_path") + context.config("shape_file_leisure")

    #get shapefiles and load into df, transforming coordinates
    df_commercial_poi = process_poi_shapefile(shape_file_commercial, ["ID", "DRUH_TXT", "TYP_TXT", "geometry"], epsg)
    df_amenity_poi = process_poi_shapefile(shape_file_amenity, ["ID", "DRUH_TXT", "TYP_TXT", "geometry"], epsg)
    df_leisure_poi = process_poi_shapefile(shape_file_leisure, ["ID", "DRUH_TXT", "TYP_TXT", "geometry"], epsg)

    #define point of interest types and load tags
    poi_types = ["work", "education", "shop", "leisure"]
    amenity = {}

    #load jisons
    for pt in poi_types:
        filename = "poi_tags_" + pt
        file = open(context.config("data_path") + context.config(filename))
        amenity[pt] = json.load(file)

    #extract amenitites coordinates and spatial join with zones

    #commercial poi
    df_shop_poi = df_commercial_poi[df_commercial_poi["type"].isin(amenity['shop'])]
    df_shops = gpd.sjoin(df_shop_poi, df_zones, op = "within", how="left").drop(columns=["index_right"])

    df_work_poi = df_commercial_poi[df_commercial_poi["type"].isin(amenity['work'])]
    df_workplaces = gpd.sjoin(df_work_poi, df_zones, op = "within", how="left").drop(columns=["index_right"])

    #amenity poi
    df_education_poi = df_amenity_poi[df_amenity_poi["type"].isin(amenity['education'])]
    df_schools = gpd.sjoin(df_education_poi, df_zones, op = "within", how="left").drop(columns=["index_right"])

    df_workplaces_am = df_amenity_poi[df_amenity_poi["type"].isin(amenity['work'])]
    df_workplaces_am = gpd.sjoin(df_workplaces_am, df_zones, op = "within", how="left").drop(columns=["index_right"])

    df_leisure = df_amenity_poi[df_amenity_poi["type"].isin(amenity['leisure'])]
    df_leisure = gpd.sjoin(df_leisure, df_zones, op = "within", how="left").drop(columns=["index_right"])
    
    #recreation poi
    df_leisure_re = df_leisure_poi[df_leisure_poi["type"].isin(amenity['leisure'])]
    df_leisure_re = gpd.sjoin(df_leisure_re, df_zones, op = "within", how="left").drop(columns=["index_right"])

    df_workplaces_re = df_leisure_poi[df_leisure_poi["type"].isin(amenity['work'])]
    df_workplaces_re = gpd.sjoin(df_workplaces_re, df_zones, op = "within", how="left").drop(columns=["index_right"])
   
    #concatenate poi
    df_workplaces = pd.concat([df_workplaces, df_workplaces_am, df_workplaces_re])
    df_leisure = pd.concat([df_leisure, df_leisure_re])

    return df_workplaces, df_schools, df_shops, df_leisure