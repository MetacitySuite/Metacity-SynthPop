import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import geometry
from tqdm import tqdm
from shapely.geometry import LineString, Point
from multiprocessing import Pool
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

    context.stage("preprocess.clean_census")
    context.stage("preprocess.clean_travel_survey")
    context.stage("preprocess.home")
    context.stage("synthesis.population.matched")
    context.stage("synthesis.locations.census_home")
    context.stage("synthesis.population.spatial.primary.locations")
    context.stage("preprocess.extract_hts_trip_chains")
    context.stage("preprocess.zones")
    
"""

"""
def export_shp(df, output_shp):
    travels = gpd.GeoDataFrame()

    travels["geometry"] = df.geometry.apply(lambda point: Point(-point.x, -point.y))
    travels["activities"] = df.count.values

    travels[travels.travels].to_file(output_shp)
    print("Saved to:", output_shp)
    return

def prepare_people(df_employed, df_students):
    df_persons = pd.DataFrame(columns=["person_id","trip_today"])
    df_traveling = df_employed[df_employed.travels_to_work == True].append(df_students[df_students.travels_to_school == True])
    print("df traveling", df_traveling.shape[0])
    df_persons.loc[:,"person_id"] = df_traveling.person_id.values

    print("Students traveling to school:\n", df_traveling.travels_to_school.value_counts())
    return df_persons, df_traveling

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

    if(row.person_id != next_row.person_id):
        return 0

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


def walk_short_distance(df_row):
    #speed = df_row.distance / df_row.trip_duration

    if(df_row.distance <= WALKING_DIST):
        return "walk"

    return df_row.traveling_mode


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
    df_persons.loc[:,"car_avail"] = merged.car_avail.values
    df_persons.loc[:,"driving_license"] = merged.driving_license.values
    del(merged)

    #impute geometry
    locations = df_activities.merge(df_traveling[["person_id","residence_id","commute_point","residence_point"]],
                                    left_on="person_id", right_on="person_id", how="left")

    df_activities.loc[:, "geometry"] = locations.apply(lambda row: return_geometry_point(row),axis=1)
    
    #impute location_id
    df_activities.loc[:, "location_id"] = locations.apply(lambda row: return_location(row),axis=1)
    
    #impute start and end time variation
    df_activities.sort_values(["person_id","activity_order"],inplace=True)
    df_activities.reset_index(inplace=True)
    df_activities.loc[:,"trip_duration"] = [return_trip_duration(row, next_row) 
                                for row, next_row in zip(df_activities.iterrows(),df_activities.shift(-1).iterrows())]

    df_activities.loc[:, "start_time"] = [return_time_variation(row, "start_time", prev_row, df_activities)for row, prev_row in zip(df_activities.iterrows(),df_activities.shift(1).iterrows())]
    df_activities.loc[:, "end_time"] = [return_time_variation(row, "end_time", prev_row, df_activities)for row, prev_row in zip(df_activities.iterrows(),df_activities.shift(1).iterrows())]

    #prepare trips
    df_ttrips = df_persons.merge(df_trips_hts,
                                    left_on="traveler_id", right_on="traveler_id", how="inner")

    df_ttrip_activity = df_ttrips
    df_ttrip_activity.loc[:,"origin"] = df_ttrip_activity.merge(df_activities, 
                                left_on=["person_id", "trip_order"], right_on=["person_id","activity_order"],
                                how="left").geometry.values
    df_ttrip_activity.loc[:,"trip_duration"] = df_ttrip_activity.merge(df_activities, 
                                left_on=["person_id", "trip_order"], right_on=["person_id","activity_order"],
                                how="left").trip_duration.values/60

    df_ttrip_activity.loc[:,"trip_order_to"] = df_ttrip_activity.trip_order + 1
    df_ttrip_activity.loc[:,"destination"] = df_ttrip_activity.merge(df_activities, 
                                left_on=["person_id", "trip_order_to"], right_on=["person_id","activity_order"],
                                how="left").geometry.values

    df_ttrip_activity.loc[:,"distance"] = df_ttrip_activity.apply(lambda row: abs(row.origin.distance(row.destination)), axis=1)
    print(df_ttrip_activity[["distance","trip_duration"]].describe())
    print(df_ttrip_activity.traveling_mode.value_counts())
    df_ttrip_activity.loc[:,"traveling_mode"] = df_ttrip_activity.apply(lambda row: walk_short_distance(row), axis=1)
    print(df_ttrip_activity.traveling_mode.value_counts())

    #drop unused columns
    df_activities = df_activities[df_activities.columns.intersection(columns)]

    df_ttrips = df_ttrip_activity[df_ttrip_activity.columns.intersection(columns_t)]

    df_persons = df_persons[df_persons.columns.intersection(["person_id","trip_today","car_avail","driving_license"])]

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


    

def execute(context):
    _, df_travelers, _ = context.stage("preprocess.clean_travel_survey")

    df_activities_hts,df_trips_hts = context.stage("preprocess.extract_hts_trip_chains")
    print("HTS activity chains:")
    print(df_activities_hts.head())

    print("HTS trips:")
    print(df_trips_hts.head())

    df_employed, df_students = context.stage("synthesis.population.spatial.primary.locations")

    print("Preparing census people for activity chains:")
    df_persons, df_traveling = prepare_people(df_employed, df_students)
    df_traveling = df_traveling.merge(df_travelers[["traveler_id","car_avail","driving_license"]],
                                    left_on="hdm_source_id", right_on="traveler_id", how="left")

    print("Persons:", df_persons.shape[0])
    print("Persons traveling (DF):", df_traveling.shape[0])
    print("Persons employed:", df_employed.shape[0])
    print("Persons students: ", df_students.shape[0])


    print("Assigning:")    
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

    #add unemployed to df_persons
    df_persons_u = pd.DataFrame()
    df_persons_u.loc[:,"person_id"] = df_u.person_id.values
    df_persons_u.loc[:,"trip_today"] = False
    df_persons_u.loc[:,"car_avail"] = df_u.car_avail.values
    df_persons_u.loc[:,"driving_license"] = df_u.driving_license.values
    
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
    print(df_persons.info())
    print(df_persons.head())
    print(df_persons.trip_today.value_counts(normalize=True))
    assert len(df_persons.person_id.unique()) == df_persons.shape[0]
    assert not df_persons.isnull().values.any()

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
    #df_ttrips.travel_mode = df_ttrips.replace("car-passenger","car")
    print(df_persons.driving_license.value_counts())
    cars = df_ttrips[df_ttrips.traveling_mode == "car"]
    #pd.set_option('display.max_rows', None)
    drivers = cars.person_id.unique() #TODO driving_lic = True if drives a car
    df_persons.loc[:, "driving_license"] = df_persons.apply(lambda row: row.driving_license or row.person_id in drivers,axis=1)
    print(df_persons.driving_license.value_counts())
    print(df_ttrips.traveling_mode.value_counts())

    print("People traveling today:")
    print(df_persons.trip_today.value_counts())

    #save to CSV files
    df_persons.to_csv(context.config("output_path")+"/df_persons.csv")
    df_activities.to_csv(context.config("output_path")+"/df_activities.csv")
    df_ttrips.to_csv(context.config("output_path")+"/df_trips.csv")
    return df_persons, df_activities, df_ttrips
