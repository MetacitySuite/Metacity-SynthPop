
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import LineString, Point, point


"""
Exporting population with travel demand to CSV files.
"""
def configure(context):
    context.config("output_path")
    context.stage("data.other.assigned_population")

def execute(context):
    df_persons, df_activities, df_trips = context.stage("data.other.assigned_population")

    #Drop coordinates
    print(df_persons.head())
    df_activities.drop(["location_id","geometry"], axis=1, inplace=True)
    print(df_activities.head())
    print(df_trips.head())

    #save to CSV files
    df_persons.to_csv(context.config("output_path")+"/csv/df_persons.csv")
    df_activities.to_csv(context.config("output_path")+"/csv/df_activities.csv")
    df_trips.to_csv(context.config("output_path")+"/csv/df_trips.csv")

    return


