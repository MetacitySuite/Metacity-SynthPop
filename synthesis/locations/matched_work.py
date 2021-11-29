from numpy.random.mtrand import seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
#import seaborn as sns
#import matplotlib.pyplot as plt


"""
This stage assigns facility id for each working person in the synthetic population 
based on the zone where the person lives, the zone-zone commute probability and the commute distance (TS) 
between residence id and candidate work destinations.

"""

def configure(context):
    context.config("seed")
    context.stage("preprocess.clean_census")
    context.stage("preprocess.home")
    context.stage("synthesis.locations.census_home")

    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_home")
    context.config("prague_area_code")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
    context.stage("preprocess.zones")
    context.stage("preprocess.extract_amenities")
    context.stage("preprocess.extract_commute_trips")
    context.stage("preprocess.extract_facility_candidates")


"""

"""

def extract_commute_distances(df_people, df_travelers, df_trips):
    distances = []
    people_travelers = df_people.merge(df_travelers, left_on="hdm_source_id", right_on="traveler_id")
    df_trips = df_trips[df_trips.traveler_id.isin(people_travelers.hdm_source_id.unique())]
    work_trips = df_trips[df_trips.destination_purpose == "work"]

    #fill in beeline - use origin, destination code (27perc missing)
    beeline_median = work_trips.groupby(['origin_code', 'destination_code'])['beeline'].transform('median')
    work_trips.loc[:, 'beeline'] = work_trips['beeline'].fillna(value=beeline_median)
    #fill rest with median
    beeline_median = work_trips['beeline'].median()
    work_trips.loc[:, 'beeline'] = work_trips['beeline'].fillna(value=beeline_median)
    

    people_travelers = people_travelers.merge(work_trips, left_on="traveler_id", right_on="traveler_id")

    #df_people["commute_distance"] = distances
    df_people = people_travelers.drop(['trip_order', 'origin_code','destination_code', 'departure_h', 'departure_m', 'arrival_h', 'arrival_m', 'traveler_id'], axis=1)

    return df_people


def assign_facility(df_people, df_facilities, seed):

    #leave unassigned people at home
    pass



def execute(context):
    df_zones = context.stage("preprocess.zones")
    df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    sample_seed = context.config("seed")

    #Assigning primary location (work): Step 1
    _, df_employed_ids = context.stage("preprocess.extract_commute_trips")
    #Assigning primary location (work): Step 2
    C_kk = context.stage("preprocess.extract_facility_candidates")
    print(C_kk.head())


    #Assigning primary location (work): Step 3

    
    df_u = df_matched.loc[df_matched.person_id.isin(df_employed_ids)]
    # extract distances
    df_u = extract_commute_distances(df_u, df_travelers, df_trips)
    print(df_u.head())
    print(df_u.beeline.value_counts())


    # assign trips in C_kk
    df_home_zones = df_u.iloc[df_employed_ids].groupby("zone_id")

    #
    #print(len(df_home_zones))

    pass