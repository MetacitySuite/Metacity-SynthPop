from typing import ValuesView
from unicodedata import normalize
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
    context.stage("preprocess.clean_travel_survey")
    context.stage("preprocess.home")
    context.stage("synthesis.population.matched")
    context.stage("synthesis.locations.census_home")
    context.stage("synthesis.locations.matched_work")
    context.stage("synthesis.locations.matched_education")
    context.stage("preprocess.extract_hts_trip_chains")

    context.stage("preprocess.zones")
    


"""

"""
def export_shp(df, output_shp):
    travels = gpd.GeoDataFrame()

    travels.loc[:,"geometry"] = df.geometry.apply(lambda point: Point(-point.x, -point.y))
    #travels.loc[:,"district_name"] = df.district_name.values
    travels.loc[:,"activities"] = df.count.values

    travels[travels.travels].to_file(output_shp)
    print("Saved to:", output_shp)
    return

def prepare_people(df_employed, df_students):
    df_persons = pd.DataFrame(columns=["person_id","trip_today"])
    df_traveling = pd.concat([df_employed, df_students])
    df_traveling.travels_to_work.fillna(False, inplace=True)
    df_traveling.travels_to_school.fillna(False, inplace=True)
    df_not_traveling = df_traveling.iloc[np.where(df_traveling.travels_to_work == False) and np.where(df_traveling.travels_to_work == False)]
    df_not_traveling.rename(columns={"residence_point":"geometry"}, inplace=True)
    print(df_not_traveling.head())
    df_traveling = df_traveling.iloc[np.where(df_traveling.travels_to_work == True) or np.where(df_traveling.travels_to_work == True)]
    df_persons.loc[:,"person_id"] = df_traveling.person_id.values
    #print(df_persons.trip_today.value_counts(normalize=True))
    return df_persons, df_traveling, df_not_traveling

def return_geometry_point(df_row):
    if(df_row.purpose == "home"):
        return Point(-df_row.residence_point.x, -df_row.residence_point.y)
    if(df_row.purpose in ["work", "education"]):
        return Point(-df_row.commute_point.x, -df_row.commute_point.y)
    return None

def return_location(df_row):
    if(df_row.purpose == "home"):
        return df_row.residence_id
    if(df_row.purpose in ["work", "education"]):
        return np.nan
    return np.nan

def return_trip_duration(row, next_row):
    ixr, row = row
    ixn, next_row = next_row
    #print(next_row)
    if(row.person_id != next_row.person_id):
        return 0
    #print(row.person_id, next_row.person_id, next_row.level_0)
    if(next_row.start_time == np.nan or row.end_time == np.nan):
        return np.nan
    
    if(next_row.start_time < row.end_time):
        midnight = 24*60*60
        return abs(next_row.start_time + (midnight - row.end_time))

    return abs(next_row.start_time - row.end_time)
    

def return_time_variation(df_row, column, prev_row=None, df = None):
    ixr, df_row = df_row
    if prev_row is None:
        return np.nan
    ixn, prev_row = prev_row
    
    if(np.isnan(df_row.loc[column])):
        return np.nan

    #print(column, df_row.loc[column], type(df_row.loc[column]))

    duration = df_row.trip_duration
    last_duration = prev_row.trip_duration
    try:
        if(last_duration == np.nan):
            last_duration = 0

        if(column == "end_time" and duration > 0):
            offset = np.random.randint(max(-30*60, -duration*0.4),min(30*60, duration*0.4))
        elif(column == "start_time" and last_duration > 0):
            offset = np.random.randint(max(-30*60, -last_duration*0.4),min(30*60, last_duration*0.4))
        else:
            offset = 0
    except ValueError:
        print(duration, last_duration, df_row[column], column)
        print(df[df.person_id == df_row.person_id][["person_id","traveler_id","purpose","start_time","end_time","trip_duration","activity_order"]])
        return df_row[column]

    return df_row[column] + offset

def assign_activities_trips(args):
    df_persons, df_traveling, df_activities_hts, df_trips_hts = args
    columns = ["person_id","purpose","start_time","end_time","geometry", "activity_order","location_id"]
    columns_t = ["person_id", "traveling_mode", "trip_order"]

    #add traveler_id to persons
    merged = df_persons.merge(df_traveling[["person_id","hdm_source_id","car_avail","driving_license"]], 
                                                    left_on="person_id", right_on="person_id", how="left")

    df_persons.loc[:,"traveler_id"] = merged.hdm_source_id.values
    df_activities = df_persons.merge(df_activities_hts,
                                    left_on="traveler_id", right_on="traveler_id", how="left")

    
    df_persons.loc[:,"trip_today"] = df_persons.apply(lambda row: (df_activities.person_id.values == row["person_id"]).sum() > 1 ,axis=1) 
    #print(df_persons.trip_today.value_counts(normalize=True))
    df_persons.loc[:,"car_avail"] = merged[["car_avail"]]
    df_persons.loc[:,"driving_license"] = merged[["driving_license"]]
    del(merged)

    #impute geometry
    #print("Imputing geometry")
    locations = df_activities.merge(df_traveling[["person_id","residence_id","commute_point","residence_point"]],
                                    left_on="person_id", right_on="person_id", how="left")

    df_activities.loc[:, "geometry"] = locations.apply(lambda row: return_geometry_point(row),axis=1)
    
    #impute location_id
    #print("Imputing location id")
    df_activities.loc[:, "location_id"] = locations.apply(lambda row: return_location(row),axis=1)
    
    #impute start and end time variation
    #print("Imputing time")
    df_activities.sort_values(["person_id","activity_order"],inplace=True)
    df_activities.reset_index(inplace=True)
    df_activities.loc[:,"trip_duration"] = [return_trip_duration(row, next_row) 
                                for row, next_row in zip(df_activities.iterrows(),df_activities.shift(-1).iterrows())]

    df_activities.loc[:, "start_time"] = [return_time_variation(row, "start_time", prev_row, df_activities)for row, prev_row in zip(df_activities.iterrows(),df_activities.shift(1).iterrows())]
    df_activities.loc[:, "end_time"] = [return_time_variation(row, "end_time", prev_row, df_activities)for row, prev_row in zip(df_activities.iterrows(),df_activities.shift(1).iterrows())]
    #print(df_activities[["person_id","purpose","start_time","end_time","trip_duration","activity_order"]].head(20))

    #drop unused columns
    df_activities = df_activities[df_activities.columns.intersection(columns)]

    #prepare trips
    df_ttrips = df_persons.merge(df_trips_hts,
                                    left_on="traveler_id", right_on="traveler_id", how="inner")


    df_ttrips = df_ttrips[df_ttrips.columns.intersection(columns_t)]
    #print(df_ttrips.columns)

    df_persons = df_persons[df_persons.columns.intersection(["person_id","trip_today","car_avail","driving_license"])]
    #print(df_persons.columns)

    #print(df_ttrips.traveling_mode.unique())
    #TODO change if child
    #df_ttrips.traveling_mode = df_ttrips.traveling_mode.replace("car-passenger","car")

    return df_activities, df_persons, df_ttrips


def assign_activities_trips_par(df_persons, df_traveling, df_activities_hts, df_trips_hts):
    cpu_available = os.cpu_count()

    df_chunks = np.array_split(df_persons.index, cpu_available)
    args = [[df_persons.iloc[df_chunk], df_traveling, df_activities_hts, df_trips_hts] for df_chunk in df_chunks]

    with Pool(cpu_available) as pool:
        results = pool.map(assign_activities_trips, tqdm(args))

    a = []
    p = []
    t = []
    for res in results:
        a.append(res[0])
        p.append(res[1])
        t.append(res[2])

    df_activities = pd.concat(a, ignore_index=True)
    df_persons = pd.concat(p)
    df_ttrips = pd.concat(t)
    return df_activities, df_persons, df_ttrips



#unused - slower and does not impute times, still good for testing
def assign_activities(df_persons, df_traveling, df_activities_hts, df_trips_hts):
    columns = ["traveler_id","purpose","start_time","end_time", "activity_order","location_id"]
    columns_t = ["traveler_id", "traveling_mode", "trip_order"]

    activity_list = []
    trip_list = []
    for ix, df in tqdm(df_persons.iterrows()):
        person_id = df.person_id
        
        df_person = df_traveling[df_traveling.person_id == person_id]
        hts_id = df_person.hdm_source_id.values[0]
        #print("HTS id:", hts_id)

        hts_activities = df_activities_hts.loc[df_activities_hts.traveler_id == hts_id].sort_values(["activity_order"])
        #print(df_activities_hts.traveler_id.unique())
        hts_activities.loc[:,"traveler_id"] = person_id
        #print(hts_activities.head())
        hts_trips = df_trips_hts.loc[df_trips_hts.traveler_id == hts_id].sort_values(["trip_order"])
        if(hts_trips.shape[0] == 0):
            df_persons.at[ix,"trip_today"] = False
        else:
            df_persons.at[ix,"trip_today"] = True
            hts_trips["traveler_id"] = person_id
            trip_list.append(hts_trips)

        
        
        #TODO add geometry for home and commute
        home = df_person.residence_point.values[0]
        home = Point(home.y, home.x)

        #print(df_p.head())
        commute = df_person.commute_point.values[0]
        commute = Point(commute.y, commute.x)

        purposes = hts_activities.purpose.values
        hts_activities["geometry"] = [home if p == "home" else commute for p in purposes ]
        hts_activities["location_id"] = [df_person.residence_id.values[0] if p == "home" else np.nan for p in purposes ]

        #TODO change time variations
        activity_list.append(hts_activities)
        

    if(len(trip_list)>0):
        df_ttrips = pd.concat(trip_list,ignore_index=True)
    else:
        df_ttrips = pd.DataFrame(columns=columns_t)

    df_ttrips.rename(columns={"traveler_id":"person_id"},inplace=True)

    df_activities = pd.concat(activity_list, ignore_index=True)
    df_activities.rename(columns={"traveler_id":"person_id"},inplace=True)
    
    return df_activities, df_persons, df_ttrips

    

def execute(context):
    _, df_travelers, _ = context.stage("preprocess.clean_travel_survey")

    df_activities_hts,df_trips_hts = context.stage("preprocess.extract_hts_trip_chains")
    print("HTS activity chains:")
    print(df_activities_hts.head())

    print("HTS trips:")
    print(df_trips_hts.head())

    df_employed = context.stage("synthesis.locations.matched_work")
    df_students = context.stage("synthesis.locations.matched_education")

    print("Preparing census people for activity chains:")
    df_persons, df_traveling, df_not_traveling = prepare_people(df_employed, df_students)
    df_traveling = df_traveling.merge(df_travelers[["traveler_id","car_avail","driving_license"]],
                                    left_on="hdm_source_id", right_on="traveler_id", how="left")

    print("Persons traveling:", df_persons.shape[0])
    print("Persons traveling (DF):", df_traveling.shape[0])
    print("Persons not traveling (DF):", df_not_traveling.shape[0])

    print("Assigning:")
    #df_activities, df_persons, df_ttrips = assign_activities(df_persons.head(5000), df_traveling, df_activities_hts, df_trips_hts)
    #df_activities, df_persons, df_ttrips = assign_activities_trips([df_persons.head(200000), df_traveling, df_activities_hts, df_trips_hts])
    print(df_traveling.head())
    print(df_traveling.columns)

    
    
    df_activities, df_persons, df_ttrips = assign_activities_trips_par(df_persons, df_traveling, df_activities_hts, df_trips_hts)
    #cca 2 min
    print("Activities to traveling census assigned.")
    print(df_persons.info())

    df_census_home = context.stage("synthesis.locations.census_home")
    df_census_matched = context.stage("synthesis.population.matched")
    df_census_matched = df_census_matched.merge(df_travelers[["traveler_id","car_avail","driving_license"]],
                                    left_on="hdm_source_id", right_on="traveler_id", how="left")

    df_census_matched.loc[:,"residence_id"] = df_census_matched.merge(df_census_home[["person_id","residence_id"]],
                                    left_on="person_id", right_on="person_id", how="left").residence_id.values

    print(df_census_matched.head())
    df_home = context.stage("preprocess.home")
    print("Assign unemployed census and assigned people who stay at home (travel not in Prague):")
    df_u = df_census_matched[~df_census_matched.person_id.isin(df_traveling.person_id.unique())]
    df_u = df_u[['person_id', 'sex', 'age', 'employment', 'residence_id', "car_avail","driving_license"]]
    df_u = df_u.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    #print("DF_U")
    #print(df_u.shape[0])
    #df_u = df_u.append(df_not_traveling)
    #df_u.reset_index(inplace=True)
    print("DF_U")
    print(df_u.shape[0])

    #add unemployed to df_persons
    df_persons_u = df_u[["person_id"]]
    df_persons_u.loc[:,"trip_today"] = False
    df_persons_u.loc[:,"car_avail"] = df_u[["car_avail"]]
    df_persons_u.loc[:,"driving_license"] = df_u[["driving_license"]]
    df_persons = df_persons.append(df_persons_u)
    df_persons.reset_index(inplace=True)
    #add unemployed to df_activities
    columns = ["person_id","purpose","start_time","end_time", "geometry","activity_order","location_id"]
    df_activities_u = pd.DataFrame(columns=columns)
    df_activities_u.loc[:,"person_id"] = df_persons_u.person_id.values
    df_activities_u.loc[:,"purpose"] = "home"
    df_activities_u.loc[:,"start_time"] = np.nan
    df_activities_u.loc[:,"end_time"] = np.nan
    df_activities_u.loc[:,"geometry"] = df_u.geometry.apply(lambda point: Point(-point.x, -point.y))
    df_activities_u.loc[:,"activity_order"] = 0
    df_activities_u.loc[:,"location_id"] = df_u.residence_id.values
    df_activities = df_activities.append(df_activities_u)
    df_activities.reset_index(inplace=True)

    


    df_persons.drop(["index"],axis=1, inplace=True) 
    df_activities.drop(["index"],axis=1, inplace=True) 

    print("PERSONS:", df_persons.shape[0])
    #print(df_persons.info())
    print(df_persons.head())
    print(df_persons.trip_today.value_counts(normalize=True))
    assert len(df_persons.person_id.unique()) == df_persons.shape[0]

    print("ACTIVITIES:", df_activities.shape[0])
    print(df_activities.info())
    print(df_activities.head())
    print(df_activities.purpose.value_counts(normalize=True))
    assert len(df_activities.person_id.unique()) == len(df_persons.person_id.unique())
    print("Person id in persons that are not in activities:",set(df_persons.person_id.unique()) - set(df_activities.person_id.unique()))
    

    print("TRIPS:")
    print(df_ttrips.info())
    print(df_ttrips.head())

    #car-passenger without drivers_lic and car avail

    return df_persons, df_activities, df_ttrips
