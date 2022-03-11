
import pandas as pd
import numpy as np
from tqdm import tqdm
import synthesis.algo.other.misc as misc

columns = ["traveler_id","purpose","start_time","end_time", "activity_order"]
columns_t = ["traveler_id", "traveling_mode", "trip_order"]
primary_activities = ["home", 'work', "education"]
variable_activities = ['shop', 'leisure', 'other']
purposes = primary_activities + variable_activities
"""
This stage extracts proper activity chains and trips of HTS travelers for MATSim export.

"""

def configure(context):
    context.stage("data.hts.clean_travel_survey")
    context.config("output_path")
    
"""
Create home activity for travelers that do not travel for education and work.

"""
def create_home_activity(traveler, columns):
    df_activities_t = pd.DataFrame(columns=columns)
    df_activities_t.loc[0,"purpose"] = "home"
    df_activities_t.loc[0,"traveler_id"] = traveler
    df_activities_t.loc[0,"start_time"] = np.nan
    df_activities_t.loc[0,"end_time"] = np.nan
    df_activities_t.loc[0,"activity_order"] = 0
    return df_activities_t
    

def extract_chain_alt(day):
    day.sort_values(["trip_order"], inplace=True)
    traveler = day["traveler_id"].values[0]
    df_activities_t = pd.DataFrame(columns=columns)
    df_trips_t = pd.DataFrame(columns=columns_t)

    start_times = []
    end_times = []
    activities = []
    trips_m = []

    start_times.append(np.nan)
    day_started = False
        
    for ix, trip in day.iterrows():
        if(not day_started): #start day
            day_started = True
            activities.append(trip.origin_purpose)

        if(trip.origin_purpose != trip.destination_purpose):
            end_times.append(trip.departure_time)
            start_times.append(trip.arrival_time)
            trips_m.append(trip.traveling_mode)
            activities.append(trip.destination_purpose)


    end_times.append(np.nan)
        
    if(len(set(activities)) > 1):
        if (activities[-1] != activities[0]):
            print(day)

        #Save activity chain to df
        df_activities_t.loc[:,"purpose"] = activities
        df_activities_t.loc[:,"traveler_id"] = traveler
        df_activities_t.loc[:,"start_time"] = start_times
        df_activities_t.loc[:,"end_time"] = end_times
        df_activities_t.loc[:,"activity_order"] = range(len(activities))
        #Save trips to df
        df_trips_t.loc[:,"traveling_mode"] = trips_m
        df_trips_t.loc[:,"trip_order"] = range(len(trips_m))
        df_trips_t.loc[:,"traveler_id"] = traveler
    else:
        df_activities_t = create_home_activity(traveler, columns)
        return df_activities_t, None

    return df_activities_t, df_trips_t
            
    

def extract_chain(day):
    day.sort_values(["trip_order"], inplace=True)
    traveler = day["traveler_id"].values[0]
    df_activities_t = pd.DataFrame(columns=columns)
    df_trips_t = pd.DataFrame(columns=columns_t)
    destinations = []
    start_times = []
    end_times = []
    activities = []
    trips_m = []
    start_times.append(np.nan)
    last_activity = np.nan
    day_started = False
    activity_duration = 0
        
    for ix, trip in day.iterrows():
        if((trip.origin_purpose in purposes) and (trip.origin_purpose != last_activity)):
            day_started = True
            activities.append(trip.origin_purpose)
            end_times.append(trip.departure_time)
            durations = misc.return_activity_duration(start_times[-1], trip.departure_time)
            last_activity = trip.origin_purpose
        if(day_started and trip.destination_purpose in purposes and ((len(activities) == 0) or (trip.destination_purpose !=activities[-1]))):
            destinations.append(trip.destination_purpose)
            start_times.append(trip.arrival_time)
            trips_m.append(trip.traveling_mode)

    if(len(end_times)>= len(start_times)):
        end_times = end_times[:len(start_times)-1]
        
    end_times.append(np.nan)
        
    if(len(destinations)>0 and len(set(activities)) > 1):
        if(destinations[-1] != activities[-1]):
            activities.append(activities[0])

        trips_m = trips_m[:len(activities)-1]

        if (activities[-1] != activities[0]):
                print(day)

        #Save activity chain to df
        df_activities_t.loc[:,"purpose"] = activities
        df_activities_t.loc[:,"traveler_id"] = traveler
        df_activities_t.loc[:,"start_time"] = start_times
        df_activities_t.loc[:,"end_time"] = end_times
        df_activities_t.loc[:,"activity_order"] = range(len(activities))
        #Save trips to df
        df_trips_t.loc[:,"traveling_mode"] = trips_m
        df_trips_t.loc[:,"trip_order"] = range(len(trips_m))
        df_trips_t.loc[:,"traveler_id"] = traveler
    else:
        df_activities_t = create_home_activity(traveler, columns)
        return df_activities_t, None

    return df_activities_t, df_trips_t
            



def export_trip_chains(df_travelers, df_trips):
    df_activities = pd.DataFrame(columns=columns)
    df_ttrips = pd.DataFrame(columns=columns_t)

    activity_list = []
    trip_list = []

    day_trips = df_trips.groupby("traveler_id")
    print("Day trips (grouped by travelers):",len(day_trips))

    for i, day in tqdm(day_trips):
        day_activities, person_trips = extract_chain_alt(day)
        activity_list.append(day_activities)
        if(trip_list != None):
            trip_list.append(person_trips)

    df_activities = pd.concat(activity_list)
    df_ttrips = pd.concat(trip_list)

    df_activities_ids = len(df_activities.traveler_id.unique())
    df_ttrips_ids = len(df_ttrips.traveler_id.unique())
    df_travelers_ids = len(df_travelers.traveler_id.unique())
    print("Df_activities:", df_activities_ids, df_activities_ids/df_travelers_ids)
    print("Df trips:", df_ttrips_ids, df_ttrips_ids/df_travelers_ids)
    #for travelers with no trips append home activity
    no_trip_travelers = set(df_travelers.traveler_id.unique()) - set(df_activities.traveler_id.unique()) 
    no_trips = df_travelers[df_travelers.traveler_id.isin(no_trip_travelers)]

    print("Assigned travelers:", len(df_activities.traveler_id.unique()))
    not_trip_activities = []
    for ix, traveler in no_trips.iterrows():
        not_trip_activities.append(create_home_activity(traveler.traveler_id, columns))

    not_trip_activities = pd.concat(not_trip_activities, ignore_index=True)
    df_activities = df_activities.append(not_trip_activities)
    df_activities.reset_index(drop = True, inplace=True)
    print("Total:",len(df_activities.traveler_id.unique()),len(df_activities.traveler_id.unique())/ df_travelers_ids)
    no_trip_hts = set(df_activities.traveler_id.unique()).difference(df_ttrips.traveler_id.unique())
    print("No trip travelers:",len(no_trip_hts), len(no_trip_hts)/len(df_activities.traveler_id.unique()))
    print("Subtraction:", set(df_activities.traveler_id.unique()).difference(set(df_travelers.traveler_id.unique())))
    
    return df_activities, df_ttrips



"""
out: activities, trips
"""
def execute(context):
    _, df_travelers, df_trips = context.stage("data.hts.clean_travel_survey")

    print("Exporting activity chains and trips from HTS:")
    activities, trips = export_trip_chains(df_travelers, df_trips) 

    assert len(df_travelers.traveler_id.unique()) == len(activities.traveler_id.unique())

    activities.to_csv(context.config("output_path")+"/csv/hts_activities_extracted.csv")
    trips.to_csv(context.config("output_path")+"/csv/hts_trips_extracted.csv")


    #a = activities.copy()
    #a["duration_m"] = a.apply(lambda row: misc.return_activity_duration(row.start_time, row.end_time), axis=1)/60
    #print(a[a.duration_m < 1.0])
    #print(len(a[a.duration_m < 1.0].traveler_id.unique()))
    #pid = 2857
    #print(a[a.traveler_id == pid].head())
    #print(trips[trips.traveler_id == pid].head())
    #pdf = df_trips[df_trips.traveler_id == pid][["trip_order","origin_purpose", "destination_purpose", "beeline", "departure_time", "arrival_time"]].sort_values(["trip_order"])
    #print(pdf)

    return activities, trips
