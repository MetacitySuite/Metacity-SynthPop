from numpy.random.mtrand import seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString
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

#def home_work_distance(home, work):
#    dist_m = np.sqrt(pow((home.x - work.x),2) + pow((home.y- work.y),2))
#    dist_km = dist_m/1000.0
#    return dist_km
    



def assign_facility(args):
    #leave unassigned people at home
    k, df_u, V, epsg = args

    df_u["workplace_point"] = np.nan
    V["assigned"] = False #picked
    V["J"] = np.inf
    

    for i, u in df_u.iterrows():
        home = u.residence_point
        distance = u.beeline
        V.J = np.inf
        V[V.assigned == False].J = V[V.assigned == False].apply(lambda v: abs(np.sqrt(pow((home.x - v.work_x),2) + pow((home.y- v.work_y),2))/1000 - distance), axis=1)
            
        #pick minimum 
        u.workplace_point = V[V.J == V.J.min()].workplace_point
        V[V.J == V.J.min()].assigned = True

    #return df_u

def assign_ordering(args):
    #leave unassigned people at home
    k, df_u, V, epsg = args
    indices = []
    f_available = np.ones((len(V),), dtype = bool)
    costs = np.ones((len(V),)) * np.inf

    commute_coordinates = np.vstack([
        V.work_x.values,
        V.work_y.values
    ]).T

    for home_coordinate, commute_distance in zip(df_u["residence_point"], df_u["beeline"]):
        distances = np.sqrt(np.sum((commute_coordinates[f_available] - home_coordinate)**2, axis = 1))
        costs[f_available] = np.abs(distances - commute_distance)

        selected_index = np.argmin(costs)
        indices.append(selected_index)
        f_available[selected_index] = False
        costs[selected_index] = np.inf

    #assert len(set(indices)) == len(V)
    #print(indices)

    return indices


def execute(context):
    #df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_home = context.stage("preprocess.home")
    df_census_home = context.stage("synthesis.locations.census_home")
    epsg = context.config("epsg")

    #Assigning primary location (work): Step 1
    _, df_employed = context.stage("preprocess.extract_commute_trips")

    #Assigning primary location (work): Step 2
    C_kk = context.stage("preprocess.extract_facility_candidates")
    C_kk["work_x"] = C_kk.workplace_point.apply(lambda point: point.x)
    C_kk["work_y"] = C_kk.workplace_point.apply(lambda point: point.y)
    print("Work trips available:",C_kk.shape[0])


    #Assigning primary location (work): Step 3
    df_u = df_employed
    print("Employed census (workers #):", df_u.shape[0])
    #print(df_u.info())
     # extract and assign trip distances from HTS to census
    
    #print(df_u.info())
    print("Employed census (workers #):", df_u.shape[0])


    #extract residence point from df_home
    print("Extract residence points:")
    df_u = df_u.merge(df_census_home[["person_id","residence_id"]], left_on="person_id", right_on="person_id", how="left")
    df_u = df_u[['person_id', 'sex', 'age', 'employment', 'residence_id', 'zone_id','district_name', 'hdm_source_id','beeline']]
    df_u = df_u.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    df_u.rename(columns = {"geometry":"residence_point"}, inplace=True)
    #print(df_u.info())
    print("Employed census (workers #):", df_u.shape[0])

 

    print("Assign primary locations:")
    df_home_zones = df_u.groupby("zone_id")

    #for each home zone k assign trips from C_kk among locals
    args = []

    results = []
    for k, df in tqdm(df_home_zones):
        C_k =  C_kk.loc[C_kk.home_zone_id == k]
        #args.append([k,df,C_k, epsg])
        print("People in zone:",df.shape[0], k)
        print("Travels in zone:", C_k.shape[0], C_k.home_zone_id.unique()[0])
        if(df.shape[0] > C_k.shape[0]):
            print("Less travels in zone than people.")
            #return

        indices = assign_ordering([k,df,C_k,epsg])
        print(len(indices))
        ordered_candidates = C_k.iloc[indices]
        ordered_candidates.loc[:,"person_id"] = df.person_id
        results.append(ordered_candidates)
        #return


    #with Pool(os.cpu_count()) as pool:
    #    results = pool.map(assign_facility, args)
    #pool.close()
    #pool.join()
    C_kk_assigned = pd.concat(results)
    df_census_assigned_workplace = df_u.merge(C_kk_assigned[["person_id","workplace_point"]], left_on = "person_id", right_on="person_id", how="left")
        
    print(df_census_assigned_workplace.info())
    return df_census_assigned_workplace
