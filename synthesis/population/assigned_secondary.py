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
    

"""

"""
    

def execute(context):
    df_persons, df_activities, df_trips = context.stage("synthesis.population.assigned")

    print(df_trips.info())
    print(df_activities.info())
    df_activities["departure_order"] = df_activities.activity_order - 1
    df_trips['arrival'] = df_trips.merge(df_activities[['start','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").start.values
    print(df_trips.info())
    #df_trips["travel_time"] = df_trips["arrival"] - df_trips["departure"]
    

    
