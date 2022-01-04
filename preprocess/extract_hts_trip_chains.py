from typing import ValuesView
from numpy.random.mtrand import hypergeometric, seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString, Point, point
from multiprocessing import Pool, cpu_count
import os
#import seaborn as sns
#import matplotlib.pyplot as plt


"""
This stage extracts proper activity chains and trips of HTS travelers for MATSim export.

"""

def configure(context):
    context.stage("preprocess.clean_travel_survey")

    


"""
Create home activity for travelers that do not travel for education and work.

"""
def create_home_activity(traveler, columns):
    df_activities_t = pd.DataFrame(columns=columns)
    df_activities_t.loc[0,"purpose"] = "home"
    df_activities_t.loc[0,"traveler_id"] = traveler
    df_activities_t.loc[0,"start_time"] = np.nan
    df_activities_t.loc[0,"end_time"] = np.nan
    df_activities_t.loc[0,"activity_order"] = 0
    return df_activities_t
    

def export_trip_chains(df_travelers, df_trips):
    columns = ["traveler_id","purpose","start_time","end_time", "activity_order"]
    columns_t = ["traveler_id", "traveling_mode", "trip_order"]
    purposes = ["home", 'work', "education"]

    df_activities = pd.DataFrame(columns=columns)
    df_ttrips = pd.DataFrame(columns=columns_t)

    activity_list = []
    trip_list = []

    for traveler, day in tqdm(df_trips.groupby("traveler_id")):
        day.sort_values(["trip_order"], inplace=True)
        df_activities_t = pd.DataFrame(columns=columns)
        df_trips_t = pd.DataFrame(columns=columns_t)
        destinations = []
        start_times = []
        end_times = []
        activities = []
        trips_m = []
        start_times.append(np.nan)
        last_activity = np.nan
        day_started = False
        for ix, trip in day.iterrows():
            if((trip.origin_purpose in purposes) and (trip.origin_purpose != last_activity)):
                day_started = True
                activities.append(trip.origin_purpose)
                end_times.append(trip.departure_time)
                last_activity = trip.origin_purpose
            if(day_started and trip.destination_purpose in purposes and ((len(activities) == 0) or (trip.destination_purpose !=activities[-1]))):
                destinations.append(trip.destination_purpose)
                start_times.append(trip.arrival_time)
                trips_m.append(trip.traveling_mode) #TODO fix proper trip modes
        if(len(end_times)>= len(start_times)):
            end_times = end_times[:len(start_times)-1]
            
        end_times.append(np.nan)
        if(len(destinations)>0 and len(set(activities)) > 1):
            if(destinations[-1] != activities[-1]):
                activities.append(destinations[-1])
            
            trips_m = trips_m[:len(activities)-1]
            #print(activities)
            #print(start_times)
            #print(end_times)
            #print(trips_m)
            
            #Save activity chain to df
            df_activities_t.loc[:,"purpose"] = activities
            df_activities_t.loc[:,"traveler_id"] = traveler
            df_activities_t.loc[:,"start_time"] = start_times
            df_activities_t.loc[:,"end_time"] = end_times
            df_activities_t.loc[:,"activity_order"] = range(len(activities))
            #Save trips to df
            df_trips_t.loc[:,"traveling_mode"] = trips_m
            df_trips_t.loc[:,"trip_order"] = range(len(trips_m))
            df_trips_t.loc[:,"traveler_id"] = traveler

            activity_list.append(df_activities_t)
            trip_list.append(df_trips_t)
        else:
            activity_list.append(create_home_activity(traveler, columns))
            

    df_activities = pd.concat(activity_list)
    df_ttrips = pd.concat(trip_list)
    #for travelers with no trips append home activity
    no_trips = df_travelers[~df_travelers.traveler_id.isin(df_activities.traveler_id.unique())]
    print("Assigned travelers:", len(df_activities.traveler_id.unique()))
    print("No trip travelers:",len(no_trips))
    print("Total:",len(df_activities.traveler_id.unique()) + len(no_trips), df_travelers.shape[0])

    not_trip_activities = []
    for ix, traveler in no_trips.iterrows():
        not_trip_activities.append(create_home_activity(traveler.traveler_id, columns))

    not_trip_activities = pd.concat(not_trip_activities, ignore_index=True)
    df_activities = df_activities.append(not_trip_activities)
    df_activities.reset_index(inplace=True)
    return df_activities, df_ttrips


"""
out: activities, trips
"""
def execute(context):
    _, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    print("Exporting activity chains and trips from HTS:")
    activities, trips = export_trip_chains(df_travelers, df_trips) 
    assert len(df_travelers.traveler_id.unique()) == len(activities.traveler_id.unique())

    return activities, trips
