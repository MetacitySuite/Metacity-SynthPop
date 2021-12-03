from typing import ValuesView
from numpy.random.mtrand import hypergeometric, seed
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


def add_activity(person_id, trip,  last_purpose, activity_order, df_traveling, last_start, last_activity=False):
    activity = {}
    purposes = ["home", 'work', "education"]
    activity["person_id"] = person_id

    if(trip.origin_purpose in (purposes) and not last_activity):
        activity["purpose"] = trip.origin_purpose
    elif last_activity:
        activity["purpose"] = trip.destination_purpose
        #print("Imputing last purpose", last_purpose)
    else:
        activity["purpose"] = last_purpose

    
    if(trip.origin_purpose in (["home"])):
        activity["geometry"] = df_traveling[df_traveling.person_id == person_id].residence_point
        activity["location_id"] = df_traveling[df_traveling.person_id == person_id].residence_id
    else:
        activity["geometry"] = df_traveling[df_traveling.person_id == person_id].commute_point
           

    trip_duration = trip.arrival_time - trip.departure_time
    activity["end_time"] = trip.departure_time + np.random.randint(max(-trip_duration/2,-30*60), min(30*60, trip_duration/2))
    activity["start_time"] = last_start
    if last_activity:
        activity["end_time"] = np.nan
        activity["start_time"] = trip.arrival_time + np.random.randint(max(-trip_duration/2,-30*60), min(30*60, trip_duration/2))

    activity["activity_order"] = activity_order
    return activity


def export_trips(df_persons, df_traveling, df_trips):
    columns = ["person_id","purpose","start_time","end_time", "geometry","activity_order","location_id"]
    df_activities = pd.DataFrame(columns=columns)
    df_trips_out = pd.DataFrame(columns=["person_id", "traveling_mode","trip_order"])
    purposes = ["home", 'work', "education"]

    count = 0
    for persons_index, df in tqdm(df_persons.iterrows()):
        count += 1
        person = df.person_id
        hts_id = df_traveling[df_traveling.person_id == person].hdm_source_id.values[0]
        person_trips = df_trips.loc[df_trips.traveler_id == hts_id].sort_values(["trip_order"])
        person_activity = []
        person_trip = []
        if (not "work" in person_trips.origin_purpose.unique()) and (not "education" in person_trips.origin_purpose.unique()):
            #print("Person stays at home")
            df_persons.at[persons_index,"trip_today"] = False
            activity = {}
            activity["person_id"] = df.person_id
            activity["purpose"] = "home"
            activity["geometry"] = df_traveling[df_traveling.person_id == person].residence_point.values[0]
            activity["activity_order"] = 0
            activity["location_id"] = df_traveling[df_traveling.person_id == person].residence_id.values[0]
            person_activity.append(activity)
        else:
            #print("Person has a trip")
            df_persons.at[persons_index,"trip_today"] = True
            trip_order = 0
            activity_order = 0
            last_purpose = "unknown"
            first_activity = np.nan
            last_activity = np.nan
            last_start = np.nan
            for ix, trip in person_trips.iterrows(): #maybe sort by time to be sure
                if(trip.origin_purpose) in (purposes): #last purpose that we leave
                    last_purpose = trip.origin_purpose #activity type
                    
                    trip_duration = trip.arrival_time - trip.departure_time
                    last_start = trip.arrival_time  + np.random.randint(max(-trip_duration/2,-30*60), min(30*60, trip_duration/2))

                if(trip.destination_purpose in (purposes) and trip.destination_purpose != last_purpose): # we leave to new valid activity

                    activity = add_activity(df.person_id, trip, last_purpose, activity_order, df_traveling, last_start)
                    if(activity_order == 0):
                        first_activity = last_purpose
                        activity["start_time"] = np.nan
                    activity_order += 1
                    person_activity.append(activity)

                    p_trip = {}
                    p_trip["person_id"] = df.person_id
                    p_trip["traveling_mode"] = trip.traveling_mode
                    p_trip["trip_order"] = trip_order
                    trip_order += 1
                    person_trip.append(p_trip)

                    if(ix == person_trips.last_valid_index()): #can be used as last trip
                        activity = add_activity(df.person_id, trip, trip.destination_purpose, activity_order, df_traveling, last_start,  True)
                        person_activity.append(activity)
                        last_activity = trip.destination_purpose
                    
                        if(last_activity != first_activity):
                            print("FA",first_activity,"LA" ,last_activity)
                            print("Person trip is not circular!")
                            print(person_trips)
                            print(person_activity)
                            print(person_trip)
                            return
                
        df_activities = df_activities.append(person_activity,ignore_index=True, sort=False)
        df_trips_out = df_trips_out.append(person_trip,ignore_index=True, sort=False)
        if(count == 3000):
            return df_activities, df_persons, df_trips_out

    #person_trips_out
    return df_activities, df_persons, df_trips_out




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

    
    #df_ttrips = pd.DataFrame(columns=["person_id","traveling_mode","trip_order"])
    print(df_trips.head())
    df_persons, df_traveling = prepare_people(df_employed, df_students)

    df_activities, df_persons, df_ttrips = export_trips(df_persons, df_traveling, df_trips)

    print(df_activities.info())
    print(df_activities.head())
    print(df_activities.purpose.value_counts(normalize=True))

    df_persons = df_persons.iloc[np.where(~df_persons.trip_today.isna())]

    print(df_persons.info())
    print(df_persons.head())

    print(df_ttrips.info())
    print(df_ttrips.head())

    #print()
    #count = 0
    #for i, df in df_activities.groupby("person_id"):
#
    #    if(df.shape[0]>1):
    #        print(i)
    #        print(df[["person_id","purpose", "activity_order","start_time","end_time"]].head())
    #        print("trips")
    #        print(df_ttrips[df_ttrips.person_id == i])
    #        count+=1
    #    if(count == 150):
    #        break
#
    #print("ACTIVITIES")
    #print(df_activities[df_activities.person_id == 1])
    #print(df_ttrips[df_ttrips.person_id == 1])
#
#


    return df_persons, df_activities, df_ttrips
