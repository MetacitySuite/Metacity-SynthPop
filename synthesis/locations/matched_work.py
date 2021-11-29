from numpy.random.mtrand import seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString
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
    context.config("prague_area_code")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
    context.stage("preprocess.zones")
    context.stage("preprocess.extract_amenities")
    context.stage("preprocess.extract_commute_trips")
    context.stage("preprocess.extract_facility_candidates")


"""

"""

def home_work_distance(epsg, home, work):
    #print(home, work)
    beeline = LineString([home, work])
    beeline.crs = epsg

    #geod = Geod()#initstring="epsg:5514")
    dist_m = beeline.length #geod.line_length(beeline, radians=False)
    dist_km = dist_m/1000.0
    #print(dist_km)
    return dist_km
    

def extract_commute_distances(df_people, df_trips):
    work_trips = df_trips[df_trips.destination_purpose == "work"]

    #fill in beeline - use origin, destination code (27perc missing)
    beeline_median = work_trips.groupby(['origin_code', 'destination_code'])['beeline'].transform('median')
    work_trips.beeline = work_trips['beeline'].fillna(value=beeline_median)
    #fill rest with median
    work_trips.beeline = work_trips['beeline'].fillna(value=work_trips['beeline'].median())
    
    print(len(df_people.hdm_source_id.isna()))
    work_trips = work_trips.loc[work_trips.traveler_id.isin(df_people.hdm_source_id.unique())]
    df_people = df_people.merge(work_trips[["traveler_id","beeline"]], 
    left_on="hdm_source_id", right_on="traveler_id")
    return df_people


def assign_facility(df_people, df_facilities, seed):

    #leave unassigned people at home
    pass



def execute(context):
    df_zones = context.stage("preprocess.zones")
    df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_home = context.stage("preprocess.home")
    df_census_home = context.stage("synthesis.locations.census_home")
    sample_seed = context.config("seed")

    #Assigning primary location (work): Step 1
    _, df_employed_ids = context.stage("preprocess.extract_commute_trips")
    #Assigning primary location (work): Step 2
    C_kk = context.stage("preprocess.extract_facility_candidates")
    #print(C_kk.head())


    #Assigning primary location (work): Step 3
    df_u = df_matched.loc[df_matched.person_id.isin(df_employed_ids)]
    print("Employed census:", df_u.shape[0])
    print(df_u.info())
     # extract and assign trip distances from HTS to census
    print("Extract beeline distances:")
    df_u = extract_commute_distances(df_u, df_trips)
    print(df_u.info())
    #print(df_u.beeline.value_counts())

    #extract residence point from df_home
    df_u = df_u.merge(df_census_home[["person_id","residence_id"]], left_on="person_id", right_on="person_id", how="left")
    df_u = df_u[['person_id', 'sex', 'age', 'employment', 'residence_id', 'zone_id','district_name', 'hdm_source_id','beeline']]
    print("Extract residence points:")
    df_u = df_u.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    df_u.rename(columns = {"geometry":"residence_point"}, inplace=True)
    print(df_u.info())


   


    # assign trips in C_kk
    print("Assign primary locations:")
    df_home_zones = df_u.loc[df_u.person_id.isin(df_employed_ids)].groupby("zone_id")

    #print(len(df_home_zones))
    #print(C_kk.head())
    args = [[k,df] for k, df in df_home_zones]

    #with Pool(os.cpu_count()) as pool:
    #    results = pool.map(assign_facility, args)

    #for each home zone k assign trips from C_kk among locals
    for k, df_u in df_home_zones: #split to Pool
        C_k = C_kk.loc[C_kk.home_zone_id == k]
        o_k = C_k.shape[0]
        #print("Demand:", o_k, "for", k)

        df_u["workplace_point"] = np.nan
        V = C_k.copy()
        V["assigned"] = False #picked
        V["J"] = np.inf
        for i, u in df_u.iterrows():
            home = u.residence_point
            distance = u.beeline
            V.J = np.inf
            for j, v in V.loc[V.assigned == False].iterrows():
                work = v.workplace_point
                v.J = abs(home_work_distance(context.config("epsg"), home, work) - distance)
            #pick minimum 
            assigned = V[V.J == V.J.min()]
            u.workplace_point = assigned.workplace_point
            V[V.J == V.J.min()].assigned = True
        
    print(df_home_zones.info())
    pass