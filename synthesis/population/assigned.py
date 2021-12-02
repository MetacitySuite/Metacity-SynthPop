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

def prepare_people(df_employed, df_students):
    df_persons = pd.DataFrame(columns=["person_id","trip_today"])
    df_traveling = pd.concat([df_employed, df_students])
    df_traveling.travels_to_work.fillna(False, inplace=True)
    df_traveling.travels_to_school.fillna(False, inplace=True)
    df_persons.loc[:,"person_id"] = df_traveling.person_id.values
    #TODO create from HTS
    #df_persons.loc[:,"trip_today"] = df_traveling.apply(lambda row: row.travels_to_work or row.travels_to_school,axis=1).values
    #print(df_persons.info())
    print(df_persons.trip_today.value_counts(normalize=True))
    return df_persons, df_traveling

def prepare_activities(df_persons, df_traveling, df_trips):
    columns = ["person_id","purpose","start_time","end_time", "geometry","activity_order","location_id"]
    df_home_from = pd.DataFrame(columns=columns)
    df_home_to = pd.DataFrame(columns=columns)
    df_home_stay = pd.DataFrame(columns=columns)
    df_work = pd.DataFrame(columns=columns)
    df_school = pd.DataFrame(columns=columns)

    print(df_trips.info())

    #prepare HOME activities
    df_home_from.loc[:, "person_id"] = df_persons[df_persons.trip_today].person_id.values
    df_home_from.loc[:,"purpose"] = "home"
    df_home_to.loc[:, "person_id"] = df_persons[df_persons.trip_today].person_id.values
    df_home_to.loc[:,"purpose"] = "home"
    df_home_stay.loc[:, "person_id"] = df_persons[~df_persons.trip_today].person_id.values
    df_home_stay.loc[:,"purpose"] = "home"

    df_home_stay.loc[:,"geometry"] = df_home_stay.merge( df_traveling[["residence_point", "person_id"]], left_on="person_id", right_on="person_id", how="left").residence_point
    df_home_stay.loc[:,"activity_order"] = 0

    #trips from home
    trips_from_home = df_trips[df_trips.origin_purpose == "home"]
    trips_from_home = trips_from_home.drop_duplicates(["traveler_id"])
    df_from_home = df_traveling.merge(trips_from_home[["traveler_id", "departure_time","trip_order"]], left_on="hdm_source_id", right_on="traveler_id", how="left")


    df_home_from.loc[:,"geometry"] = df_home_from.merge( df_traveling[["residence_point", "person_id"]], left_on="person_id", right_on="person_id", how="left").residence_point
    df_home_from.loc[:,"end_time"] = df_home_from.merge( df_from_home[["person_id", "departure_time"]], left_on="person_id", right_on="person_id", how="left").departure_time
    df_home_from.loc[:,"activity_order"] = df_home_from.merge( df_from_home[["person_id", "trip_order"]], left_on="person_id", right_on="person_id", how="left").trip_order - 1

    #trips to home
    trips_to_home = df_trips[df_trips.destination_purpose == "home"]
    trips_to_home = trips_to_home.drop_duplicates(["traveler_id"])
    df_to_home = df_traveling.merge(trips_from_home[["traveler_id", "arrival_time","trip_order"]], left_on="hdm_source_id", right_on="traveler_id", how="left")


    df_home_to.loc[:,"geometry"] = df_home_to.merge( df_traveling[["residence_point", "person_id"]], left_on="person_id", right_on="person_id", how="left").residence_point
    df_home_to.loc[:,"start_time"] = df_home_to.merge( df_to_home[["person_id", "arrival_time"]], left_on="person_id", right_on="person_id", how="left").arrival_time
    df_home_to.loc[:,"activity_order"] = df_home_to.merge( df_to_home[["person_id", "trip_order"]], left_on="person_id", right_on="person_id", how="left").trip_order - 1

    #prepare work activities
    df_work.loc[:,"person_id"] = df_traveling[df_traveling.travels_to_work].person_id.values
    df_work.loc[:,"geometry"] = df_traveling[df_traveling.travels_to_work].commute_point.values
    df_work.loc[:,"purpose"] = "work"
    trips_work_to = df_trips[df_trips.destination_purpose == "work"]
    trips_work_from = df_trips[df_trips.origin_purpose == "work"]

    df_to_work = df_traveling[df_traveling.travels_to_work].merge(trips_work_to[["traveler_id","arrival_time","trip_order"]], left_on="hdm_source_id", right_on="traveler_id", how="left")
    df_from_work = df_traveling[df_traveling.travels_to_work].merge(trips_work_from[["traveler_id","departure_time","trip_order"]], left_on="hdm_source_id", right_on="traveler_id", how="left")

    df_work.loc[:,"start_time"] = df_work.merge(df_to_work, left_on="person_id", right_on="person_id", how="left").arrival_time.values
    df_work.loc[:,"end_time"] = df_work.merge(df_from_work, left_on="person_id", right_on="person_id", how="left").departure_time.values
    df_work.loc[:,"activity_order"] = df_work.merge(df_to_work, left_on="person_id", right_on="person_id", how="left").trip_order.values - 1

    trips_from_home = trips_from_home.drop_duplicates(["traveler_id"])





    df_activities = pd.concat([df_home_from, df_home_to, df_home_stay, df_work, df_school])
    #df_activities.location_id.fillna([None], inplace=True)
    print(df_activities.head())
    print((df_activities[["start_time","end_time","activity_order"]]).describe())

def export_trips(df_persons, df_traveling, df_trips):
    columns = ["person_id","purpose","start_time","end_time", "geometry","activity_order","location_id"]
    purposes = ["home", 'work', "education"]
    hts_ids = df_traveling.hdm_source_id.unique()

    for person in df_persons.person_id.values[1:]:
        print("trip today:",df_persons[df_persons.person_id == person].trip_today.values)
        print(df_traveling[df_traveling.person_id == person])
        hts_id = df_traveling[df_traveling.person_id == person].hdm_source_id[0]
        print(person, hts_id)
        person_trips = df_trips.loc[df_trips.traveler_id == hts_id].sort_values(["trip_order"])
        person_activity = pd.DataFrame(columns=columns)
        print(person_trips)
        #if not ("work".isin(person_trips.origin_purpose)) and not ("education".isin(person_trips.origin_purpose)):
        #    df_persons[]

        #person_trips_out
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

    
    
    
    df_ttrips = pd.DataFrame(columns=["person_id","traveling_mode","trip_order"])

    
    df_persons, df_traveling = prepare_people(df_employed, df_students)

    df_activities = export_trips(df_persons, df_traveling, df_trips)



    

    
    return df_persons, df_activities, df_ttrips
