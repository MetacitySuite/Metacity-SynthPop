import numpy as np
import pandas as pd
import tqdm


def configure(context):
    context.config("prague_area_code")
    context.stage("data.hts.clean_travel_survey")
    context.stage("synthesis.population.matched")

def extract_primary_activity_distances(df_people, df_trips):
    print("Extracting commute distances:")
    trips = df_trips.loc[df_trips.traveler_id.isin(df_people.hdm_source_id.unique())]
    trips = trips.drop_duplicates(["traveler_id"])
    print("\tHTS trips to impute:", trips.shape[0])
    df_people = df_people.merge(trips[["traveler_id","beeline"]], 
                    left_on="hdm_source_id", right_on="traveler_id")
    print("\tCensus travelers with beeline:",df_people.shape[0])
    return df_people

def extract_work_distances(df_people, df_trips):
    print("Extracting commute distances:")

    print("\tCensus workers traveling:",df_people.shape[0])
    work_trips = df_trips.loc[df_trips.traveler_id.isin(df_people.hdm_source_id.unique())]
    work_trips = work_trips.drop_duplicates(["traveler_id"])
    print("\tWork trips to impute:", work_trips.shape[0])
    df_people = df_people.merge(work_trips[["traveler_id","beeline"]], 
                    left_on="hdm_source_id", right_on="traveler_id")
    print("\tCensus workers with beeline:",df_people.shape[0])
    return df_people



def execute(context):
    df_matched = context.stage("synthesis.population.matched")
    _, _, df_trips = context.stage("data.hts.clean_travel_survey")

    employed = df_matched[df_matched.employment == "employed"]
    #extract employed people who travel to work in Prague area
    #traveler ids with work trips in Prague
    work_trips = df_trips[df_trips.destination_purpose == "work"].copy()
    prague_area = context.config("prague_area_code")

    #work trips outside prague
    trips_outside = work_trips[work_trips.destination_code != prague_area].index.to_list() + work_trips[work_trips.origin_code != prague_area].index.to_list()

    prague_trips = work_trips.drop(trips_outside)
    hts_workers_prg = prague_trips.traveler_id.unique()
    hts_workers_out = df_trips.iloc[trips_outside].traveler_id.unique()
    hts_workers_prg = list(set(hts_workers_prg) - (set(hts_workers_out)))

    print("Workers in Prague HTS trips:",len(hts_workers_prg))
    print("Workers outside with HTS trips:",len(hts_workers_out))

    #print(df_matched.head())
    employed = df_matched.loc[df_matched.employment == "employed"]
    print("Employed in census:", len(employed))
    #print("Employed (unique HTS ids) in census:", len(employed.hdm_source_id.unique()))

    employed_no_trip = employed[~employed.hdm_source_id.isin(work_trips.traveler_id.unique())]

    employed_some_trip = employed[employed.hdm_source_id.isin(work_trips.traveler_id.unique())]
    print("Employed with no valid HTS trip:", len(employed_no_trip))

    employed_out_trip = employed_some_trip[employed_some_trip.hdm_source_id.isin(hts_workers_out)]
    print("Employed with outside HTS trip:", len(employed_out_trip))

    employed_trip = employed_some_trip[employed_some_trip.hdm_source_id.isin(hts_workers_prg)]
    print("Employed (traveling) people in zones:",employed_trip.shape[0])


    unemployed = df_matched.loc[df_matched.employment != "employed"]
    unemployed_work_trip_prg = unemployed[unemployed.hdm_source_id.isin(hts_workers_prg)]
    unemployed_work_trip_out = unemployed[unemployed.hdm_source_id.isin(hts_workers_out)]
    print("UNemployed with valid HTS trip:", len(unemployed_work_trip_prg))
    print("UNemployed with outside HTS trip:", len(unemployed_work_trip_out))
    print("Unemployed valid trip split\n", unemployed_work_trip_prg.employment.value_counts())

    assert(employed.shape[0] == (employed_trip.shape[0]+employed_no_trip.shape[0]+employed_out_trip.shape[0]))

    #assign commute points for employed primary_trip a employed_no_trip
    df_assign_workplace = pd.concat([employed_trip, employed_no_trip, unemployed_work_trip_prg])
    
    df_leave_home = pd.concat([employed_out_trip, unemployed_work_trip_out])

    ids = set(df_assign_workplace.person_id.to_list() + df_leave_home.person_id.to_list())
    #print(len(set(df_assign_workplace.person_id.to_list() ).intersection(set(df_leave_home.person_id.to_list()))))

    df_other = df_matched[~df_matched.person_id.isin(ids)]

    assert df_matched.shape[0] == (df_assign_workplace.shape[0]+df_other.shape[0]+df_leave_home.shape[0])

    return df_assign_workplace, df_other, df_leave_home




    




