
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import LineString, Point
import synthesis.algo.other.misc as misc


"""
Exporting assigned activity coords to shapefiles.
"""
def configure(context):
    context.config("output_path")
    context.stage("synthesis.population.output")

def prepare_trips(df_activities, df_trips):
    df_activities["departure_order"] = df_activities.activity_order - 1

    df_trips['start'] = df_trips.merge(df_activities[['end_time','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").end_time.values

    df_trips['preceeding_purpose'] = df_trips.merge(df_activities[['purpose','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").purpose.values

    df_trips['preceeding_purpose_coords'] = df_trips.merge(df_activities[['purpose','person_id','activity_order', 'geometry']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").geometry.values

    df_trips['end'] = df_trips.merge(df_activities[['start_time','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").start_time.values

    df_trips['following_purpose'] = df_trips.merge(df_activities[['purpose','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").purpose.values

    df_trips['following_purpose_coords'] = df_trips.merge(df_activities[['purpose','person_id','departure_order', 'geometry']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").geometry.values

    df_trips.loc[:,"travel_time"] = df_trips.apply(lambda row: misc.return_trip_duration(row.start, row.end), axis=1) 

    df_trips.loc[:,"beeline"] = df_trips.apply(lambda row: row.preceeding_purpose_coords.distance(row.following_purpose_coords), axis=1)

    df_trips.rename(columns={"traveling_mode":"mode"}, inplace=True)

    #trip_id
    df_trips["trip_id"] = df_trips.trip_order.values
    df_trips = df_trips.sort_values(["person_id", "trip_id"])

    return df_trips

def execute(context):
    df_persons, df_activities, df_trips = context.stage("synthesis.population.output")
    df_activities.loc[:,"duration_m"] = df_activities.apply(lambda row: misc.return_activity_duration(row.start_time, row.end_time), axis=1)/60
    
    df_trips = prepare_trips(df_activities, df_trips)
    #for p,df in df_activities.groupby(df_activities.purpose):
    #    print("Purpose:",p)
    #    print(df.describe())

    #Drop coordinates
    print(df_persons.head())
    print(df_activities.head())
    print(df_trips.head())

    #save to CSV files
    df_persons.to_csv(context.config("output_path")+"/df_persons.csv")
    df_activities.to_csv(context.config("output_path")+"/df_activities.csv")
    df_trips.to_csv(context.config("output_path")+"/df_trips.csv")

    return df_persons, df_activities, df_trips


