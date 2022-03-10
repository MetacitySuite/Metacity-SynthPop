import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm

from shapely.geometry import LineString, Point

#import seaborn as sns
#import matplotlib.pyplot as plt


"""
This stage assigns facility id for each working person in the synthetic population 
based on the zone where the person lives, the zone-zone commute probability and the commute distance (TS) 
between residence id and candidate work destinations.

"""

def assign_ordering(args):
    #leave unassigned people at home
    k, df_u, V, epsg = args
    indices = []
    f_available = np.ones((len(V),), dtype = bool)
    costs = np.ones((len(V),)) * np.inf

    commute_coordinates = np.vstack([
        V.commute_x.values,
        V.commute_y.values
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

def export_shp(df, output_shp):
    travels = gpd.GeoDataFrame()

    travels.loc[:,"geometry"] = df.apply(lambda row: LineString([row.residence_point, row.commute_point]), axis=1)
    travels.loc[:,"person_id"] = df.person_id.values
    travels.loc[:,"beeline"] = df.beeline.values
    #travels.loc[:,"district_name"] = df.district_name.values
    travels.loc[:,"travelerid"] = df.hdm_source_id.values
    travels.loc[:,"travels"] = df.commute_point.apply(lambda point: not point.equals(Point(-1.0,-1.0)))

    travels[travels.travels].to_file(output_shp)
    print("Saved to:", output_shp)
    return

def configure(context):
    context.config("data_path")
    context.config("output_path")
    context.config("epsg")

    context.stage("data.spatial.home")
    context.stage("synthesis.spatial.primary.census_home")
    context.stage("synthesis.spatial.primary.extract_commute_trips")
    context.stage("synthesis.spatial.primary.candidates")


def process(context, purpose):
    #TODO
    df_home = context.stage("data.spatial.home")
    df_census_home = context.stage("synthesis.spatial.primary.census_home")
    
    epsg = context.config("epsg")

    #Assigning primary location (purpose): Step 1
    #Assigning primary location (purpose): Step 2
    if(purpose == "work"):
        C_kk, _ = context.stage("synthesis.spatial.primary.candidates")
        _, df_employed, _, _ = context.stage("synthesis.spatial.primary.extract_commute_trips")
        df_u = df_employed
    if(purpose == "edu"):
        _, C_kk = context.stage("synthesis.spatial.primary.candidates")
        _, _, _, df_students = context.stage("synthesis.spatial.primary.extract_commute_trips")
        df_u = df_students

    C_kk["commute_x"] = C_kk.commute_point.apply(lambda point: point.x)
    C_kk["commute_y"] = C_kk.commute_point.apply(lambda point: point.y)
    print(purpose,"trips available:",C_kk.shape[0])

    #Assigning primary location (work): Step 3
    print("Assigning people:", df_u.shape[0])

    #extract residence point from df_home
    print("Extract residence points:")
    df_u = df_u.merge(df_census_home[["person_id","residence_id"]], left_on="person_id", right_on="person_id", how="left")
    df_u = df_u[['person_id', 'sex', 'age', 'employment', 'residence_id', 'zone_id','district_name', 'hdm_source_id','beeline']]
    df_u = df_u.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    df_u.rename(columns = {"geometry":"residence_point"}, inplace=True)

    print("Assign primary locations:")
    df_home_zones = df_u.groupby("zone_id")

    #for each home zone k assign trips from C_kk among locals
    results = []
    for k, df in tqdm(df_home_zones):
        C_k =  C_kk.loc[C_kk.home_zone_id == k]
        
        if(df.shape[0] > C_k.shape[0]):
            print("People in zone:",df.shape[0], k)
            print("Travels in zone:", C_k.shape[0], C_k.home_zone_id.unique()[0])
            print("Less travels in zone than people.")
            #return

        indices = assign_ordering([k,df,C_k,epsg])
        ordered_candidates = C_k.iloc[indices]
        if(df.shape[0] > ordered_candidates.shape[0]):
            print("People in zone:",df.shape[0], k)
            print("Assigned in zone:", ordered_candidates.shape[0], ordered_candidates.home_zone_id.unique()[0])
            print("Less travels in zone than people.")

        ordered_candidates.loc[:,"person_id"] = df["person_id"].values
        results.append(ordered_candidates)
      
    C_kk_assigned = pd.concat(results)
    df_census_assigned = df_u.merge(C_kk_assigned[["person_id","commute_point"]], left_on = "person_id", right_on="person_id", how="left")
    
    if(purpose == "work"):
        df_census_assigned.loc[:,"travels_to_work"] = df_census_assigned.commute_point.apply(lambda point: not point.equals(Point(-1.0,-1.0)))
    if(purpose == "edu"):
        df_census_assigned.loc[:,"travels_to_school"] = df_census_assigned.commute_point.apply(lambda point: not point.equals(Point(-1.0,-1.0)))
    

    #result validation and export to shp
    geometries = df_census_assigned.commute_point.apply(lambda x: x.wkt).values
    print("Unique workplaces:", len(set(geometries)))
    export_shp(df_census_assigned, context.config("output_path")+purpose+"_travels.shp")
    return df_census_assigned

"""

"""
def execute(context):
    df_workers = process(context, "work")
    df_students = process(context, "edu")

    df_workers.rename(columns={"commute_point":"workplace_point"}, inplace=True)
    df_students.rename(columns={"commute_point":"school_point"}, inplace=True)

    df_workers = df_workers.merge(df_students[["person_id","school_point"]], left_on="person_id", right_on="person_id", how="left")
    df_students = df_students.merge(df_workers[["person_id","workplace_point"]], left_on="person_id", right_on="person_id", how="left")

    return df_workers, df_students
