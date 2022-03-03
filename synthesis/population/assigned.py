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
    context.stage("synthesis.population.assign_primary")
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
    df_traveling = df_employed[df_employed.travels_to_work == True]
    df_traveling = df_traveling.append(df_students[df_students.travels_to_school == True])
    df_traveling = df_traveling.drop_duplicates(subset="person_id")

    #print("df traveling", df_traveling.shape[0])
    df_persons.loc[:,"person_id"] = df_traveling.person_id.values
    #print("Students traveling to school:\n", df_traveling.travels_to_school.value_counts())

    return df_persons, df_traveling

def return_geometry_point(df_row):
    if(df_row.purpose == "home"):
        return Point(-df_row.residence_point.x, -df_row.residence_point.y)
    if(df_row.purpose in ["work", "education"]): #TODO shift to main function
        try:
            return Point(-df_row.commute_point.x, -df_row.commute_point.y)
        except:
            #print(df_row.traveler_id)
            return None
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
    shift = 15*60

    try:
        if(last_duration == np.nan):
            last_duration = 0

        if(column == "end_time" and duration > 0):
            offset = np.random.randint(max(-shift, -duration*0.4),min(shift, duration*0.4))
        elif(column == "start_time" and last_duration > 0):
            offset = np.random.randint(max(-shift, -last_duration*0.4),min(shift, last_duration*0.4))
        else:
            offset = 0
    except ValueError:
        print(duration, last_duration, df_row[column], column)
        print(df[df.person_id == df_row.person_id][["person_id","traveler_id","purpose","start_time","end_time","trip_duration","activity_order"]])
        return df_row[column]

    new_time = df_row[column] + offset
    midnight = 24*60*60

    if(new_time > midnight):
        return midnight
    
    if(new_time < 0):
        return 0

    return new_time


def walk_short_distance(df_row):
    #speed = df_row.distance / df_row.trip_duration
    if(df_row.distance != np.nan and df_row.distance <= WALKING_DIST):
        return "walk"

    return df_row.traveling_mode

def get_distance(origin, destination):
    if(origin == None or destination == None):
        return np.nan
    else:
        try:
            return abs(origin.distance(destination))
        except:
            print(origin, destination)


def assign_activities_other(args):
    df_persons, df_activities_hts, df_trips_hts = args
    columns = ["person_id","purpose","start_time","end_time","geometry", "activity_order","location_id"]
    columns_t = ["person_id", "traveling_mode", "trip_order", "beeline"]

    df_persons.loc[:,"traveler_id"] = df_persons.hdm_source_id.values
    df_activities = df_persons.merge(df_activities_hts, left_on="traveler_id", right_on="traveler_id", how="left")

    df_persons.loc[:,"trip_today"] = df_persons.apply(lambda row: (df_activities.person_id.values == row["person_id"]).sum() > 1 ,axis=1) 

    #impute geometry
    locations = df_activities.copy()
    #.merge(df_persons[["person_id","residence_point"]],
    #left_on="person_id", right_on="person_id", how="left",suffixes=("",""))

    print(locations.info())
    print(df_activities.purpose.unique())

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

    df_activities.loc[:,"trip_duration"] = [return_trip_duration(row, next_row) 
                                for row, next_row in zip(df_activities.iterrows(),df_activities.shift(-1).iterrows())]
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

    #TODO in secondary we know only trip duration
    df_ttrip_activity.loc[:,"distance"] = np.nan #df_ttrip_activity.apply(lambda row: get_distance(row.origin, row.destination), axis=1)
    #df_ttrip_activity.loc[:,"beeline"] = df_persons.merge(df_trips_hts,
    #                                left_on="traveler_id", right_on="traveler_id", how="inner")
    #print(df_ttrip_activity[["distance","trip_duration"]].describe())
    #print(df_ttrip_activity.traveling_mode.value_counts())
    df_ttrip_activity.loc[:,"traveling_mode"] = df_ttrip_activity.apply(lambda row: walk_short_distance(row), axis=1)
    #print(df_ttrip_activity.traveling_mode.value_counts())

    #drop unused columns
    df_activities = df_activities[df_activities.columns.intersection(columns)]
    df_ttrips = df_ttrip_activity[df_ttrip_activity.columns.intersection(columns_t)]
    df_persons = df_persons[df_persons.columns.intersection(["person_id","trip_today","car_avail","driving_license"])]

    return df_activities, df_persons, df_ttrips


def assign_other_activities_trips_par(df_persons, df_activities_hts, df_trips_hts):
    cpu_available = os.cpu_count()
    
    df_chunks = np.array_split(df_persons.index, cpu_available)
    args = [[df_persons.iloc[df_chunk], df_activities_hts, df_trips_hts] for df_chunk in df_chunks]

    with Pool(cpu_available) as pool:
        results = pool.map(assign_activities_other, tqdm(args))

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

def prepare_unemployed(df_census, df_traveling, df_home, df_hts):
    df_unemployed = df_census[df_census.employment == "unemployed"]
    df_u = df_unemployed[['person_id','hdm_source_id', 'sex', 'age', 'employment', 'residence_id', "car_avail","driving_license"]]
    print(len(set(df_u.person_id.values).intersection(set(df_traveling.person_id.values))))
    df_other = df_census[df_census.employment != "unemployed"][['person_id','hdm_source_id', 'sex', 'age', 'employment', 'residence_id', "car_avail","driving_license"]]
    df_other = df_other[~df_other.person_id.isin(df_traveling.person_id.values)]
    df_other.loc[:,'trip_today'] = df_other.merge(df_hts,left_on="hdm_source_id", right_on="traveler_id", how="left").trip_today.values
    print(len(set(df_other.person_id.values).intersection(set(df_traveling.person_id.values))))
    df_travels = df_other[df_other.trip_today == True]
    df_stays_home = df_other[df_other.trip_today == False]
    print(len(set(df_travels.person_id.values).intersection(set(df_stays_home.person_id.values))))
    
    df_u = df_u.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    df_travels = df_travels.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    df_stays_home = df_stays_home.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")

    df_u.rename(columns={"geometry":"residence_point"}, inplace=True)
    df_travels.rename(columns={"geometry":"residence_point"}, inplace=True)
    df_stays_home.rename(columns={"geometry":"residence_point"}, inplace=True)
    print("Unemployed in census:",df_u.shape[0])
    print("Non-traveling employed and students:",df_travels.shape[0])
    print("Trav employed and students:",df_stays_home.shape[0])
    return df_u, df_travels, df_stays_home
    
def filter_workers(df_u, df_activities_hts):

    hts_active = df_activities_hts[df_activities_hts.purpose.isin(["work","education"])].traveler_id.values
    hts_active = list(set(hts_active))

    df_active = df_u[df_u.hdm_source_id.isin(hts_active)]
    df_true = df_u[~df_u.hdm_source_id.isin(hts_active)]
    df_true.reset_index(drop=True, inplace=True)

    return df_true, df_active

def execute(context):
    _, df_travelers, _ = context.stage("preprocess.clean_travel_survey")
    df_census_home = context.stage("synthesis.locations.census_home")
    df_census_matched = context.stage("synthesis.population.matched")
    df_activities_hts,df_trips_hts = context.stage("preprocess.extract_hts_trip_chains")
    df_traveling, df_persons, df_activities, df_ttrips = context.stage("synthesis.population.assign_primary")
    df_employed, df_students = context.stage("synthesis.population.spatial.primary.locations")
    df_home = context.stage("preprocess.home")
    print("Already assigned:",df_persons.shape[0])
    
    #TODO assign activity chains to the rest of the people
    df_census_matched = df_census_matched.merge(df_travelers[["traveler_id","car_avail","driving_license"]],
                                    left_on="hdm_source_id", right_on="traveler_id", how="left")
    df_census_matched.loc[:,"residence_id"] = df_census_matched.merge(df_census_home[["person_id","residence_id"]],
                                    left_on="person_id", right_on="person_id", how="left").residence_id.values

    
    
    print(df_census_matched.employment.value_counts())
    print(df_census_matched.shape[0])
    df_u, df_travels, df_home = prepare_unemployed(df_census_matched, df_persons, df_home, df_travelers)
    
    print("Together:", df_u.shape[0]+df_persons.shape[0]+df_travels.shape[0]+df_home.shape[0])
    print("Assign unemployed census:")
    #TODO filter out people with 
    df_u, df_u_active = filter_workers(df_u, df_activities_hts)
    print("Unemployed (true):", df_u.shape[0])
    print("Unemployed (active):", df_u_active.shape[0])
    
    print("Keeping home:", df_home.shape[0]+ df_travels.shape[0]+df_u_active.shape[0])
    print("Traveling:", df_u.shape[0]+df_persons.shape[0])

    df_activities_o, df_persons_o, df_trips_o = assign_other_activities_trips_par(df_u, df_activities_hts, df_trips_hts)
    
    print("Assign people who stay at home (travel not in Prague):")
    df_o = df_home.copy().append(df_u_active).append(df_travels)
    df_o.reset_index(inplace=True)
    print(df_o.info())
    print(df_o.head())
    print(df_activities.head())

    #add unemployed to df_persons
    df_persons_u = pd.DataFrame()
    df_persons_u.loc[:,"person_id"] = df_o.person_id.values
    df_persons_u.loc[:,"trip_today"] = False
    df_persons_u.loc[:,"car_avail"] = df_o.car_avail.values
    df_persons_u.loc[:,"driving_license"] = df_o.driving_license.values
    
    df_persons = df_persons.append(df_persons_u)
    df_persons.reset_index(inplace=True)
    #add unemployed to df_activities
    columns = ["person_id","purpose","start_time","end_time", "geometry","activity_order","location_id"]
    df_activities_u = pd.DataFrame(columns=columns)
    df_activities_u.loc[:,"person_id"] = df_o.person_id.values
    df_activities_u.loc[:,"purpose"] = "home"
    df_activities_u.loc[:,"start_time"] = np.nan
    df_activities_u.loc[:,"end_time"] = np.nan
    df_activities_u.loc[:,"geometry"] = df_o.residence_point.apply(lambda point: Point(-point.x, -point.y))
    df_activities_u.loc[:,"activity_order"] = 0
    df_activities_u.loc[:,"location_id"] = df_o.residence_id.values

    df_activities = df_activities.append(df_activities_u)
    df_activities.reset_index(inplace=True)


    df_persons = df_persons.append(df_persons_o)
    df_activities = df_activities.append(df_activities_o)
    df_ttrips = df_ttrips.append(df_trips_o)
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

 
    return df_persons, df_activities, df_ttrips
