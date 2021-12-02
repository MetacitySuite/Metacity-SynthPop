from typing import ValuesView
from numpy.random.mtrand import seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString, Point
from multiprocessing import Pool
import os
#import seaborn as sns
#import matplotlib.pyplot as plt


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

    context.stage("preprocess.clean_census")
    context.stage("preprocess.home")
    context.stage("synthesis.locations.census_home")
    context.stage("synthesis.locations.matched_work")
    context.stage("synthesis.locations.matched_education")
    
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
    context.stage("preprocess.zones")
    


"""

"""
def export_shp(df, output_shp):

    return



def execute(context):
    #df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_home = context.stage("preprocess.home")
    df_census_home = context.stage("synthesis.locations.census_home")
    epsg = context.config("epsg")

    df_employed = context.stage("synthesis.locations.matched_work")
    df_students = context.stage("synthesis.locations.matched_education")

    print("Working students:")
    print(df_employed[df_employed.person_id.isin(df_students.person_id.unique())].info())

    df_traveling = df_employed.append(df_students)
    df_persons = pd.DataFrame(columns=["person_id","trip_today"])
    df_activities = pd.DataFrame(columns=["person_id","type","start_T","end_T", "geometry","activity_order","facility_id"])
    df_ttrips = pd.DataFrame(columns=["person_id","traveling_mode","trip_order"])

    #prepare df_persons
    df_persons[:,"person_id"] = df_traveling.person_id.values
    #df_persons[:,"trip_today"] = 
    print(df_persons.describe())

    

    
    return df_persons, df_activities, df_ttrips
