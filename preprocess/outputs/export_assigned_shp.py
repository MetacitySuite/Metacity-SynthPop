
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import LineString, Point, point



"""
Exporting assigned activity coords to shapefiles.
"""
def configure(context):
    context.config("output_path")
    context.stage("synthesis.population.assigned")


def export_shp(df, output_shp):
    travels = gpd.GeoDataFrame(df)
    travels.loc[:,"geometry"] = travels.geometry.apply(lambda point: Point(-point.x, -point.y))
    travels.to_file(output_shp)
    print("Saved to:", output_shp)
    return


def export_activity(df, activity, context):
    print("Exporting activity shapefile:", activity)
    df_a_home = df[df.purpose == activity].copy()
    df_a_home.drop_duplicates(["person_id"], inplace=True) # only one point per activity type per person
    df_a_home = df_a_home[df_a_home.location_id != np.nan]

    df_a = pd.DataFrame()
    df_a["geometry"] = df_a_home.geometry
    export_shp(df_a, context.config("output_path")+"activities_"+activity+".shp")
    return


def execute(context):
    _, df_activities, _ = context.stage("synthesis.population.assigned")
    export_activity(df_activities, "home", context)
    export_activity(df_activities, "work", context)
    export_activity(df_activities, "education", context)
    return


