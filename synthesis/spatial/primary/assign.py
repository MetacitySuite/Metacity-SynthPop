import pandas as pd
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool
import os

import synthesis.algo.other.misc as misc
import synthesis.algo.other.shp_exporter

"""
This stage assigns valid activities and chains to people in the census traveling to primary locations such as work and education.
"""

def configure(context):
    context.config("seed")
    context.config("data_path")
    context.config("output_path")
    context.config("epsg")

    context.stage("data.hts.clean_travel_survey")
    context.stage("synthesis.spatial.primary.locations")
    context.stage("data.hts.extract_hts_trip_chains")
    
"""

"""
def export_shp(df, output_shp):
    synthesis.algo.other.shp_exporter.export_activity(df, output_shp)



def prepare_people(df_employed, df_students):
    df_persons = pd.DataFrame(columns=["person_id","trip_today"])
    
    df_traveling = df_employed#[df_employed.travels_to_work == True]
    df_traveling = df_traveling.append(df_students)#[df_students.travels_to_school == True])
    df_traveling = df_traveling.drop_duplicates(subset="person_id")

    df_persons.loc[:,"person_id"] = df_traveling.person_id.values

    return df_persons, df_traveling


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
    locations = df_activities.merge(df_traveling[["person_id","residence_id","workplace_point","school_point","residence_point"]],
                                    left_on="person_id", right_on="person_id", how="left")
    df_activities.loc[:, "geometry"] = locations.apply(lambda row:misc.return_geometry_point(row),axis=1)

    #impute location_id - for residences, otherwise NaN
    df_activities.loc[:, "location_id"] = locations.apply(lambda row: misc.return_home_id(row),axis=1)
    #impute start and end time variation
    df_activities.sort_values(["person_id","activity_order"],inplace=True)
    df_activities.reset_index(inplace=True)
    df_activities.loc[:,"trip_duration"] = [misc.return_trip_duration_row(row, next_row) 
                                for row, next_row in zip(df_activities.iterrows(),df_activities.shift(-1).iterrows())]

    df_activities.loc[:, "start_time"] = [misc.return_time_variation(row, "start_time", prev_row, df_activities)for row, prev_row in zip(df_activities.iterrows(),df_activities.shift(1).iterrows())]
    df_activities.loc[:, "end_time"] = [misc.return_time_variation(row, "end_time", prev_row, df_activities)for row, prev_row in zip(df_activities.iterrows(),df_activities.shift(1).iterrows())]

    df_activities.loc[:,"trip_duration"] = [misc.return_trip_duration_row(row, next_row) 
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

    df_ttrip_activity.loc[:,"distance"] = df_ttrip_activity.apply(lambda row: misc.get_distance(row.origin, row.destination), axis=1)
    df_ttrip_activity.loc[:,"traveling_mode"] = df_ttrip_activity.apply(lambda row: misc.walk_short_distance(row), axis=1)

    #drop unused columns
    df_activities = df_activities[df_activities.columns.intersection(columns)]

    df_ttrips = df_ttrip_activity[df_ttrip_activity.columns.intersection(columns_t)]

    df_persons = df_persons[df_persons.columns.intersection(["person_id","trip_today","car_avail","driving_license"])]

    return df_activities, df_persons, df_ttrips


def assign_chains_par(df_persons, df_traveling, df_activities_hts, df_trips_hts):
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
    _, df_travelers, _ = context.stage("data.hts.clean_travel_survey")

    df_activities_hts,df_trips_hts = context.stage("data.hts.extract_hts_trip_chains")
    print("HTS activity chains:")
    print(df_activities_hts.head())

    print("HTS trips:")
    print(df_trips_hts.head())

    df_employed, df_students = context.stage("synthesis.spatial.primary.locations")

    print("Preparing workers and students for activity chains:")
    df_persons, df_traveling = prepare_people(df_employed, df_students)

    df_traveling = df_traveling.merge(df_travelers[["traveler_id","car_avail","driving_license"]],
                                    left_on="hdm_source_id", right_on="traveler_id", how="left")

    print("Persons:", df_persons.shape[0])
    print("Persons traveling (DF):", df_traveling.shape[0])
    print("Persons employed:", df_employed.shape[0])
    print("Persons students: ", df_students.shape[0])

    print("Assigning:")    
    df_activities, df_persons, df_ttrips = assign_chains_par(df_persons, df_traveling, df_activities_hts, df_trips_hts)
    print("Activities to traveling census assigned.")

    print("Person id in persons that are not in activities:",set(df_persons.person_id.unique()) - set(df_activities.person_id.unique()))

    misc.print_assign_results(df_persons, df_activities, df_ttrips)

    return df_traveling, df_persons, df_activities, df_ttrips