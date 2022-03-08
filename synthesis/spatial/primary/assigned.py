import pandas as pd
import numpy as np
from tqdm import tqdm
from shapely.geometry import LineString, Point
from multiprocessing import Pool
import os
import synthesis.algo.other.shp_exporter
import synthesis.algo.other.misc as misc

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

    context.stage("data.census.clean_census")
    context.stage("data.hts.clean_travel_survey")
    context.stage("data.hts.extract_hts_trip_chains")
    context.stage("data.spatial.zones")
    context.stage("data.spatial.home")

    context.stage("synthesis.population.matched")
    context.stage("synthesis.spatial.primary.census_home")

    context.stage("synthesis.spatial.primary.assign")

    context.stage("data.other.census_workers")
    context.stage("data.other.census_students")

    
    
"""

"""

def export_activity_shp(df, output_shp):
    synthesis.algo.other.shp_exporter.export_activity(df, output_shp)

def assign_activities_other(args):
    df_persons, df_activities_hts, df_trips_hts = args
    columns = ["person_id","purpose","start_time","end_time","geometry", "activity_order","location_id"]
    columns_t = ["person_id", "traveling_mode", "trip_order", "beeline"]

    df_persons.loc[:,"traveler_id"] = df_persons.hdm_source_id.values
    df_activities = df_persons.merge(df_activities_hts, left_on="traveler_id", right_on="traveler_id", how="left")

    df_persons.loc[:,"trip_today"] = df_persons.apply(lambda row: (df_activities.person_id.values == row["person_id"]).sum() > 1 ,axis=1) 

    #impute geometry
    locations = df_activities.copy()

    df_activities.loc[:, "geometry"] = locations.apply(lambda row: misc.return_geometry_point(row),axis=1)
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

    #TODO in secondary we know only trip duration
    df_ttrip_activity.loc[:,"distance"] = np.nan
    df_ttrip_activity.loc[:,"traveling_mode"] = df_ttrip_activity.apply(lambda row: misc.walk_short_distance(row), axis=1)

    #drop unused columns
    df_activities = df_activities[df_activities.columns.intersection(columns)]
    df_ttrips = df_ttrip_activity[df_ttrip_activity.columns.intersection(columns_t)]
    df_persons = df_persons[df_persons.columns.intersection(["person_id","trip_today","car_avail","driving_license"])]

    return df_activities, df_persons, df_ttrips


def assign_chains_par(df_persons, df_activities_hts, df_trips_hts):
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
    _, df_travelers, _ = context.stage("data.hts.clean_travel_survey")
    df_activities_hts,df_trips_hts = context.stage("data.hts.extract_hts_trip_chains")

    df_census_home = context.stage("synthesis.spatial.primary.census_home")
    df_census_matched = context.stage("synthesis.population.matched")
    
    df_traveling, df_persons, df_activities, df_ttrips = context.stage("synthesis.spatial.primary.assign") # already assigned workers and students

    df_home = context.stage("data.spatial.home")
    print("Already assigned:",df_persons.shape[0])
    
    #TODO assign activity chains to unemployed
    _, df_other_w, df_leave_home_w = context.stage("data.other.census_workers")
    df_students, df_other_e, df_leave_home_e = context.stage("data.other.census_students")

    df_census_matched = df_census_matched.merge(df_travelers[["traveler_id","car_avail","driving_license"]],
                                    left_on="hdm_source_id", right_on="traveler_id", how="left")
    df_census_matched.loc[:,"residence_id"] = df_census_matched.merge(df_census_home[["person_id","residence_id"]],
                                    left_on="person_id", right_on="person_id", how="left").residence_id.values

    #df_u, df_travels, df_home = prepare_unemployed(df_census_matched, df_persons, df_home, df_travelers)
    leave_home_ids = list(set(df_leave_home_w.person_id.unique()).union(set(df_leave_home_e.person_id.unique())))
    print("Leaving home (#):", len(leave_home_ids))
    leave_home = df_census_matched[df_census_matched.person_id.isin(leave_home_ids)]
    
    other_ids = set(df_other_w.person_id.unique()).union(set(df_other_e.person_id.unique())) - set(leave_home_ids)
    other_ids = other_ids - set(df_persons.person_id.unique())
    print("Other in census (#):", len(other_ids))
    df_u = df_census_matched[df_census_matched.person_id.isin(other_ids)]
    
    #print("Together:", df_u.shape[0]+df_persons.shape[0]+df_travels.shape[0]+df_home.shape[0])

    print("Together:", df_u.shape[0]+df_persons.shape[0]+leave_home.shape[0])
    print("Full census:", df_census_matched.shape[0])
    print("Difference:", df_census_matched.shape[0] - (df_u.shape[0]+df_persons.shape[0]+leave_home.shape[0]))
    

    print("Overlap assigned and leave home:", len(set(leave_home_ids).intersection(df_persons.person_id.unique())))
    print("Overlap assigned and to assign:", len(set(other_ids).intersection(df_persons.person_id.unique())))

    
    to_drop = list(set(leave_home_ids).intersection(df_persons.person_id.unique()))
    #TODO add students dropped when assigning school because of zone outside probabilitites
    unassinged_students = set(df_students.person_id.unique()) - set(df_persons.person_id.unique())
    print("Unassinged students to leave home:", len(unassinged_students))
    return

    #leave_home = leave_home.append(df_census_matched[df_census_matched.person_id.isin(to_drop)])
    
    #remove from already assigned
    df_persons = df_persons[~df_persons.person_id.isin(to_drop)]
    df_activities = df_activities[~df_activities.person_id.isin(to_drop)]
    df_ttrips = df_ttrips[~df_ttrips.person_id.isin(to_drop)]
    print("Overlap assigned and leave home:", len(set(leave_home_ids).intersection(df_persons.person_id.unique())))

    assert(df_census_matched.shape[0] == (df_u.shape[0]+df_persons.shape[0]+leave_home.shape[0]))
    print("Difference:", df_census_matched.shape[0] -(df_u.shape[0]+df_persons.shape[0]+leave_home.shape[0]))


    df_u = df_u[['person_id','hdm_source_id', 'sex', 'age', 'employment', 'residence_id', "car_avail","driving_license"]]
    leave_home = leave_home[['person_id','hdm_source_id', 'sex', 'age', 'employment', 'residence_id', "car_avail","driving_license"]]

    df_u = df_u.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")
    leave_home = leave_home.merge(df_home[["residence_id","geometry"]], left_on="residence_id", right_on="residence_id", how="left")

    df_u.rename(columns={"geometry":"residence_point"}, inplace=True)
    leave_home.rename(columns={"geometry":"residence_point"}, inplace=True)

    print("Assign 'other' census:")
    #Some unemployed people still travel to work or education (???) TODO
    #df_u, df_u_active = filter_workers(df_u, df_activities_hts)
    #print("Unemployed (true):", df_u.shape[0])
    #print("Unemployed (active):", df_u_active.shape[0])
    #print("Keeping home:", df_home.shape[0]+ df_travels.shape[0]+df_u_active.shape[0])
    #print("Traveling:", df_u.shape[0]+df_persons.shape[0])

    df_activities_o, df_persons_o, df_trips_o = assign_chains_par(df_u, df_activities_hts, df_trips_hts)
    
    print("Assign people who stay at home (travel not in Prague):")
    df_o = leave_home.copy() #df_home.copy().append(df_u_active).append(df_travels)
    df_o.reset_index(inplace=True)

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
    df_activities_u.loc[:,"geometry"] = df_o.residence_point.apply(lambda point: Point([-point.x, -point.y]))
    df_activities_u.loc[:,"activity_order"] = 0
    df_activities_u.loc[:,"location_id"] = df_o.residence_id.values

    df_activities = df_activities.append(df_activities_u)
    df_activities.reset_index(inplace=True)


    df_persons = df_persons.append(df_persons_o)
    df_activities = df_activities.append(df_activities_o)
    df_ttrips = df_ttrips.append(df_trips_o)
    df_persons.drop(["index"],axis=1, inplace=True) 
    df_activities.drop(["index"],axis=1, inplace=True) 

    misc.print_assign_results(df_persons, df_activities, df_ttrips)

    assert(df_census_matched.shape[0] == df_persons.shape[0])
    print("Activity chains with primary destinations assigned.")
    
    #df_a = df_activities.copy()
    #df_a.loc[:,"duration_m"] = df_a.apply(lambda row: misc.return_activity_duration(row.start_time, row.end_time), axis=1)/60
    #for p,df in df_a.groupby(df_a.purpose):
    #    print("Purpose:",p)
    #    print(df.describe())

    return df_persons, df_activities, df_ttrips
