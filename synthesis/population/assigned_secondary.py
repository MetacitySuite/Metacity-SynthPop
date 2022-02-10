from typing import ValuesView
from unicodedata import normalize
from numpy.random.mtrand import hypergeometric, seed
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import geometry
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString, Point, point
from multiprocessing import Pool, cpu_count
import os
#import seaborn as sns
#import matplotlib.pyplot as plt

WALKING_DIST = 50 #150


"""
This stage assigns facility id for each working person in the synthetic population 
based on the zone where the person lives, the zone-zone commute probability and the commute distance (TS) 
between residence id and candidate work destinations.

"""

def configure(context):
    context.config("seed")
    context.config("data_path")
    context.config("output_path")
    context.config("epsg")

    context.stage("synthesis.population.assigned")
    context.stage("preprocess.secondary")
    

def return_trip_duration(start_time, end_time):
    if(start_time == np.nan or end_time == np.nan):
        return np.nan
    
    if(start_time < end_time):
        midnight = 24*60*60
        return abs(start_time + (midnight - end_time))

    return abs(start_time - end_time)

"""

"""
def execute(context):
    df_persons, df_activities, df_trips = context.stage("synthesis.population.assigned")

    #trips
    df_activities["departure_order"] = df_activities.activity_order - 1
    df_trips['start'] = df_trips.merge(df_activities[['end_time','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").end_time.values

    df_trips['end'] = df_trips.merge(df_activities[['start_time','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").start_time.values

    df_trips.loc[:,"travel_time"] = df_trips.apply(lambda row: return_trip_duration(row.start, row.end), axis=1) 
    print(df_trips.describe())
    print(df_activities.describe())
    print(df_activities.purpose.unique()) #TODO export all activities

    # primary locations TODO to dict


    #destinations TODO to dict
    df_destinations = context.stage("preprocess.secondary")
    #df_trips["travel_time"] = df_trips["arrival"] - df_trips["departure"]
    

    
