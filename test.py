import pandas as pd
import geopandas as gpd
import json


epsg = "EPSG:25832"

#zones - ZSJ
df_zones = gpd.read_file("./data/shp/SHP_kodZSJ/KodUzemi_Praha.shp")
df_zones = df_zones.to_crs(epsg)
df_zones = df_zones[["kodUzemi", "geometry"]]

#commercial shapefile
df_commercial_poi = gpd.read_file("./data/shp/VYBAVENOST/FSV_KomerVybav_b.shp")
df_commercial_poi = df_commercial_poi.to_crs(epsg)
df_commercial_poi = df_commercial_poi[["ID", "DRUH_TXT", "geometry"]]

#amenity shapefile
df_amenity_poi = gpd.read_file("./data/shp/VYBAVENOST/FSV_ObcanVybav_b.shp")
df_amenity_poi = df_amenity_poi.to_crs(epsg)
df_amenity_poi = df_amenity_poi[["ID", "DRUH_TXT", "geometry"]]

#leisure shapefile
df_leisure_poi = gpd.read_file("./data/shp/VYBAVENOST/FSV_RekreVybav_b.shp")
df_leisure_poi = df_leisure_poi.to_crs(epsg)
df_leisure_poi = df_leisure_poi[["ID", "DRUH_TXT", "geometry"]]

#load jisons
shop_file = open("./data/tags-shp/shop.json")
amenity_shop = json.load(shop_file)

work_file = open("./data/tags-shp/work.json")
amenity_work = json.load(work_file)

edu_file = open("./data/tags-shp/edu.json")
amenity_edu = json.load(edu_file)

leisure_file = open("./data/tags-shp/leisure.json")
amenity_leisure = json.load(leisure_file)

#extract amenitites
#commercial - shop
df_shop_poi = df_commercial_poi[df_commercial_poi["DRUH_TXT"].isin(amenity_shop)]
df_shops = gpd.sjoin(df_shop_poi, df_zones, op = "within", how="left")

#commercial - work
df_work_poi = df_commercial_poi[df_commercial_poi["DRUH_TXT"].isin(amenity_work)]
df_workplaces = gpd.sjoin(df_work_poi, df_zones, op = "within", how="left")

#amenity - education
df_edu_poi = df_amenity_poi[df_amenity_poi["DRUH_TXT"].isin(amenity_edu)]
df_schools = gpd.sjoin(df_edu_poi, df_zones, op = "within", how="left")

#amenity - work
df_workplaces_am = df_amenity_poi[df_amenity_poi["DRUH_TXT"].isin(amenity_work)]
df_workplaces_am = gpd.sjoin(df_workplaces_am, df_zones, op = "within", how="left")

#amenity - leisure
df_leisure = df_amenity_poi[df_amenity_poi["DRUH_TXT"].isin(amenity_leisure)]
df_leisure = gpd.sjoin(df_leisure, df_zones, op = "within", how="left")

#recreation - leisure
df_leisure_re = df_leisure_poi[df_leisure_poi["DRUH_TXT"].isin(amenity_leisure)]
df_leisure_re = gpd.sjoin(df_leisure_re, df_zones, op = "within", how="left")

#recreation - work
df_workplaces_re = df_leisure_poi[df_leisure_poi["DRUH_TXT"].isin(amenity_work)]
df_workplaces_re = gpd.sjoin(df_workplaces_re, df_zones, op = "within", how="left")


df_workplaces = pd.concat([df_workplaces, df_workplaces_am, df_workplaces_re])
df_leisure = pd.concat(df_leisure, df_leisure_re)

