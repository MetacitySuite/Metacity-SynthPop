import pandas as pd
import geopandas as gpd

def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_zones")
    context.config("district_file")

def execute(context):
    epsg = context.config("epsg")
    df_zones = gpd.read_file(context.config("data_path") + context.config("shape_file_zones"))
    df_zones = df_zones.to_crs(epsg)
    df_zones = df_zones[["kodUzemi", "geometry"]]
    df_zones.columns = ["zone_id", "geometry"] #rename columns
    df_zones.loc[:, 'zone_id'] = df_zones['zone_id'].astype(int)

    #add centroid information
    df_zones["zone_centroid"] = df_zones.geometry.apply(lambda geom: geom.centroid)

    #add district information
    df_district = pd.read_csv(context.config("data_path") + context.config("district_file"), delimiter=";")
    df_district = df_district.drop(['OBJECTID', 'KOD_ZSJ', 'NAZ_ZSJ'], axis = 1)
    df_district.columns = ['zone_id', 'district_id', 'district_name']
    df_zones_district = df_zones.merge(df_district, on='zone_id')

    print(df_zones_district.head(2))
    
    
    return df_zones_district