from typing import ValuesView
from numpy.random.mtrand import hypergeometric, seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString, Point
from multiprocessing import Pool, cpu_count
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
    #print(df_persons.trip_today.value_counts(normalize=True))
    return df_persons, df_traveling


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
           

    trip_duration = max(0, trip.arrival_time - trip.departure_time)
    activity["end_time"] = trip.departure_time + np.random.randint(max(-trip_duration/2,-30*60), min(30*60, trip_duration/2))
    activity["start_time"] = last_start
    if last_activity:
        activity["end_time"] = np.nan
        activity["start_time"] = trip.arrival_time + np.random.randint(max(-trip_duration/2,-30*60), min(30*60, trip_duration/2))

    activity["activity_order"] = activity_order
    return activity


def export_trips(args):
    df_persons, df_traveling, df_trips = args
    columns = ["person_id","purpose","start_time","end_time", "geometry","activity_order","location_id"]
    purposes = ["home", 'work', "education"]

    count = 0
    df_a = []
    df_to = []
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
            last_activity = first_activity = "home"
        else:
            #print("Person has a trip")
            df_persons.at[persons_index,"trip_today"] = True
            trip_order = 0
            activity_order = 0
            first_activity = np.nan
            last_activity = np.nan
            last_start = np.nan
            for ix, trip in person_trips.iterrows(): #maybe sort by time to be sure
                if(trip.origin_purpose) in (purposes): #last purpose that we leave
                    last_purpose = trip.origin_purpose #activity type
                    
                    trip_duration = max(0, trip.arrival_time - trip.departure_time)
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

                    if(trip.destination_purpose == first_activity and trip_order > 1 and not person_trips.loc[ix: , "destination_origin"] in list(set(purposes).remove(first_activity))):
                        #were finished for today
                        person_activity[-1]["end_time"] = np.nan
                        last_activity = trip.destination_purpose
                        break


                    if(ix == person_trips.last_valid_index()): #can be used as last trip
                        activity = add_activity(df.person_id, trip, trip.destination_purpose, activity_order, df_traveling, last_start,  True)
                        person_activity.append(activity)
                        last_activity = trip.destination_purpose
                
            
        if(last_activity != first_activity):
            print("FA",first_activity,"LA" ,last_activity)
            print("Person trip is not circular!")
            print(person_trips)
            trp = pd.DataFrame()
            print("Saved activities")
            trp = trp.append(person_activity, ignore_index=True)
            print(trp)
            print("Saved trips")
            print(person_trip)
            break

        else:
            df_a.extend(person_activity)
            df_to.extend(person_trip)
            if(count == -3000):
                print(len(df_a))
                break

    df_activities = pd.DataFrame(columns=columns)
    df_activities = df_activities.append(df_a,ignore_index=True, sort=False)
    df_trips_out = pd.DataFrame(columns=["person_id", "traveling_mode","trip_order"])
    df_trips_out = df_trips_out.append(df_to,ignore_index=True, sort=False)
    return df_activities, df_persons, df_trips_out




def export_trips_parallel(df_persons, df_traveling, df_trips):
    cpu_available = os.cpu_count()
    df_p_chunks = np.array_split(df_persons, cpu_available)
    args = [ [df_p, df_traveling, df_trips] for df_p in df_p_chunks]

    with Pool(cpu_available) as pool:
        results =  pool.map(export_trips ,args)

    pool.close()
    pool.join()

    df_persons = pd.concat(results[:,0])
    df_activities = pd.concat(results[:,1])
    df_ttrips = pd.concat(results[:,2])
    return df_persons, df_activities, df_ttrips

def export_trip_chains(df_travelers, df_trips):
    columns = ["traveler_id","purpose","start_time","end_time", "activity_order"]
    columns_t = ["traveler_id", "traveling_mode", "trip_order"]
    purposes = ["home", 'work', "education"]

    df_activities = pd.DataFrame(columns=columns)

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
        for ix, trip in day.iterrows():
            if(trip.origin_purpose in purposes):
                activities.append(trip.origin_purpose)
                end_times.append(trip.departure_time)
                trips_m.append(trip.traveling_mode)
            if(trip.destination_purpose in purposes):
                destinations.append(trip.destination_purpose)
                start_times.append(trip.arrival_time)
        end_times.append(np.nan)
        activities.append(destinations[-1])
        print(day)
        print(activities)
        print(start_times)
        print(end_times)
        df_activities_t.loc[:,"purpose"] = activities
        df_activities_t.loc[:,"traveler_id"] = traveler
        df_activities_t.loc[:,"start_time"] = start_times
        df_activities_t.loc[:,"end_time"] = end_times
        df_activities_t.loc[:,"activity_order"] = range(len(activities))
        print(df_activities_t.head())

        df_trips_t.loc[:,"trip_order"] = range(len(activities)-1)
        df_trips_t.loc[:,"traveler_id"] = traveler
        df_trips_t.loc[:,"traveling_mode"] = trips_m
        print(df_trips_t.head())

        df_activities = df_activities.append(df_activities_t)
        break

    #for travelers with no trips append home activity
    return df_activities, df_trips




def execute(context):
    #df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_home = context.stage("preprocess.home")
    df_census_home = context.stage("synthesis.locations.census_home")
    epsg = context.config("epsg")

    df_employed = context.stage("synthesis.locations.matched_work")
    df_students = context.stage("synthesis.locations.matched_education")

    #print("Working students:")
    #print(df_employed[df_employed.person_id.isin(df_students.person_id.unique())].info())

    
    #df_ttrips = pd.DataFrame(columns=["person_id","traveling_mode","trip_order"])
    #print(df_trips.head())
    df_persons, df_traveling = prepare_people(df_employed, df_students)


    _,_ = export_trip_chains(df_travelers, df_trips) 

    #df_activities, df_persons, df_ttrips = export_trips([df_persons[df_persons.person_id.isin([41135,1])], df_traveling, df_trips])
    #df_activities, df_persons, df_ttrips = export_trips_parallel(df_persons, df_traveling, df_trips)

    #print(df_activities.info())
    #print(df_activities.head())
    #print(df_activities.purpose.value_counts(normalize=True))
#
    #df_persons = df_persons.iloc[np.where(~df_persons.trip_today.isna())]
#
    #print(df_persons.info())
    #print(df_persons.head())
#
    #print(df_ttrips.info())
    #print(df_ttrips.head())

    #print()
    #count = 0
    #for i, df in df_activities.groupby("person_id"):
    #    if(df.shape[0]>1):
    #        print(i)
    #        print(df[["person_id","purpose", "activity_order","start_time","end_time"]].head())
    #        print("trips")
    #        print(df_ttrips[df_ttrips.person_id == i])
    #        count+=1
    #    if(count == 150):
    #        break

    #print("ACTIVITIES")
    #print(df_activities[df_activities.person_id == 1])
    #print(df_ttrips[df_ttrips.person_id == 1])
    #print(df_traveling[df_traveling.person_id == 1])
    #print(df_travelers[df_travelers.traveler_id == 1178])
    #print(df_trips[df_trips.traveler_id == 1178])
#
    #print(df_ttrips.traveling_mode.unique())
#
    #df_ttrips.traveling_mode = df_ttrips.traveling_mode.replace("car-passenger","car")
    #print(df_ttrips.traveling_mode.unique())


    #return df_persons, df_activities, df_ttrips
