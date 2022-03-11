import pandas as pd
import numpy as np
import synthesis.algo.other.misc as misc


"""
#TODO

"""

def configure(context):
    context.stage("data.spatial.secondary")
    context.stage("synthesis.spatial.primary.assigned")
    


def prepare_locations(df_activities):
    # Load persons and their primary locations
    df_home = pd.DataFrame()
    df_home =  df_activities[df_activities.purpose == "home"]
    df_home = df_home.drop_duplicates(subset="person_id", keep="first")
    df_home.rename(columns={"geometry":"home"}, inplace=True)
    print(df_home.info())

    df_work = pd.DataFrame()
    df_work =  df_activities[df_activities.purpose == "work"]
    df_work = df_work.drop_duplicates(subset="person_id", keep="first")
    df_work.rename(columns={"geometry":"work"}, inplace=True)
    print(df_work.info())

    df_education = pd.DataFrame()
    df_education =  df_activities[df_activities.purpose == "education"]
    df_education = df_education.drop_duplicates(subset="person_id", keep="first")
    df_education.rename(columns={"geometry":"education"}, inplace=True)
    print(df_education.info())

    df_locations = df_home[["person_id","home"]].copy()
    df_locations = pd.merge(df_locations, df_work[["person_id", "work"]], how = "left", on = "person_id")
    df_locations = pd.merge(df_locations, df_education[["person_id", "education"]], how = "left", on = "person_id")

    return df_locations[["person_id", "home", "work", "education"]].sort_values(by = "person_id")



def prepare_secondary(df_destinations):
    df_destinations.rename(columns = {"location_id": "destination_id"}, inplace = True)

    identifiers = df_destinations["destination_id"].values
    locations = np.vstack(df_destinations["geometry"].apply(lambda x: np.array([-x.x, -x.y])).values) 
    ## kladny 5514, jako df_primary ano nedava to smysl

    data = {}

    for purpose in ("shop", "leisure", "other"):
        f = df_destinations["offers_%s" % purpose].values

        data[purpose] = dict(
            identifiers = identifiers[f],
            locations = locations[f]
        )

    return data

def prepare_trips(df_trips, df_activities):
    df_activities["departure_order"] = df_activities.activity_order - 1
    df_trips['start'] = df_trips.merge(df_activities[['end_time','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").end_time.values

    df_trips['preceeding_purpose'] = df_trips.merge(df_activities[['purpose','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").purpose.values

    df_trips['end'] = df_trips.merge(df_activities[['start_time','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").start_time.values

    df_trips['following_purpose'] = df_trips.merge(df_activities[['purpose','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").purpose.values

    df_trips.loc[:,"travel_time"] = df_trips.apply(lambda row: misc.return_trip_duration(row.start, row.end), axis=1) 

    df_trips.rename(columns={"traveling_mode":"mode"}, inplace=True)
    df_trips["trip_id"] = df_trips.trip_order.values

    FIELDS = ["person_id", "trip_id", "preceeding_purpose", "following_purpose", "mode", "travel_time"] #TODO
    
    df_trips = df_trips[FIELDS]
    df_trips = df_trips.sort_values(["person_id", "trip_id"])
    return df_trips


"""

"""
def execute(context):
    df_persons, df_activities, df_trips = context.stage("synthesis.spatial.primary.assigned")
    df_destinations = context.stage("data.spatial.secondary")

    #trips
    df_trips = prepare_trips(df_trips, df_activities)
    # primary locations
    df_primary = prepare_locations(df_activities)
    
    # secondary destinations
    destinations = prepare_secondary(df_destinations)

    return df_trips, df_primary, destinations



    



