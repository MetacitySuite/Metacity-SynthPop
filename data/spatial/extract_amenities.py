import pandas as pd
import geopandas as gpd
import json
from tqdm import tqdm 

def load_poi_tags(file):
    poi_tags_file = open(file, encoding='utf-8')
    return json.load(poi_tags_file)
     
def process_poi_shapefile(file, col, epsg):
    gdf_poi = gpd.read_file(file, encoding='utf-8')
    gdf_poi = gdf_poi[col]
    gdf_poi = gdf_poi.to_crs(epsg)
    gdf_poi.columns = ["id", "type", "description", "geometry"]
    return gdf_poi

def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_commercial")
    context.config("shape_file_amenity")
    context.config("shape_file_leisure")
    context.config("poi_tags_work")
    context.config("poi_tags_education")
    context.config("poi_tags_shop")
    context.config("poi_tags_leisure")
    context.config("poi_tags_other")
    context.config("project_directory")

    context.stage("data.spatial.zones")

def extract_pois(amenity, df_commercial_poi, df_amenity_poi, df_leisure_poi, df_zones):
    commercial = df_commercial_poi[df_commercial_poi["description"].isin(amenity)]
    amenities = df_amenity_poi[df_amenity_poi["description"].isin(amenity)]
    leisures = df_leisure_poi[df_leisure_poi["description"].isin(amenity)]
    df_pois = pd.concat([commercial, amenities, leisures], ignore_index=True)
    df = gpd.sjoin(df_pois, df_zones, op = "within", how="left").drop(columns=["index_right"])
    return df


def execute(context):
    epsg = context.config("epsg")
    df_zones = context.stage("data.spatial.zones")
    #print("Zones:", df_zones.shape[0])

    shape_file_commercial = context.config("data_path") + context.config("shape_file_commercial")
    shape_file_amenity = context.config("data_path") + context.config("shape_file_amenity")
    shape_file_leisure = context.config("data_path") + context.config("shape_file_leisure")

    #get shapefiles and load into df, transforming coordinates
    df_commercial_poi = process_poi_shapefile(shape_file_commercial, ["ID", "DRUH_TXT", "TYP_TXT", "geometry"], epsg)
    df_amenity_poi = process_poi_shapefile(shape_file_amenity, ["ID", "DRUH_TXT", "TYP_TXT", "geometry"], epsg)
    df_leisure_poi = process_poi_shapefile(shape_file_leisure, ["ID", "DRUH_TXT", "TYP_TXT", "geometry"], epsg)

    #define point of interest types and load tags
    poi_types = ["work", "education", "shop", "leisure","other"]
    amenity = {}

    #load jsons
    for pt in poi_types:
        filename = "poi_tags_" + pt
        file = open(context.config("project_directory") +"tags/"+ context.config(filename), encoding='utf-8')
        amenity[pt] = json.load(file)

    #extract amenitites coordinates and spatial join with zones
    results = []

    for activity in poi_types:
        results.append(extract_pois(amenity[activity], df_commercial_poi, df_amenity_poi, df_leisure_poi, df_zones))


    return results #as in: df_workplaces, df_schools, df_shops, df_leisure, df_other