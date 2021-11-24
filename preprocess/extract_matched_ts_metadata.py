import numpy as np
import pandas as pd


def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_home")
    context.config("prague_area_code")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
    context.stage("preprocess.zones")
    context.stage("preprocess.extract_amenities")
    context.stage("preprocess.clean_commute_prob")
    

def extract_travelling_workers(df_trips, df_matched, prague_area):
     #traveler ids with work trips
    hts_workers = df_trips.iloc[np.where(df_trips.destination_purpose == "work") 
                            and np.where(df_trips.destination_code == prague_area) 
                            and np.where(df_trips.origin_code == prague_area) ].traveler_id.unique()

    #print("Workers in HTS:",len(hts_workers))
    #print(df_matched.head())
    employed = df_matched.loc[df_matched.employment == "employed"]
    #print("Employed in census:", len(employed))
    no_trip = employed[~employed.hdm_source_id.isin(hts_workers)]
    #print("Employed with no HTS trip:", len(no_trip))
    employed_trip = employed[employed.hdm_source_id.isin(hts_workers)]
    return employed_trip

def extract_travel_demands(employed_trip):
    O_k = {}
    employed_zones = employed_trip.groupby("zone_id")
    #print("Workers in zones (#)",len(employed_zones))
    for i, zone in employed_zones:
        O_k[i] = len(zone)
    return O_k

    

def extract_trip_counts(demand_k, pi_k):
    f_kk = pd.DataFrame()
    f_kk.columns = ["origin", "destination", "trip_count"]

    f_kk[str(o_k1)] = np.random.multinomial(demand_k, pi_k)



def execute(context):
    df_zones = context.stage("preprocess.zones")
    df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_workplaces, df_schools, df_shops, df_leisure = context.stage("preprocess.extract_amenities")
    for col in ["district_id", "zone_id"]:
        df_workplaces[col] = df_workplaces[col].fillna(-1).astype(int)
        print("Missing",col, len(df_workplaces.iloc[np.where(df_workplaces[col] == -1)]))

    
    #create activity chains
    print(df_travelers.head())
    print(df_trips.head())

    employed_trip = extract_travelling_workers(df_trips, df_matched, context.config("prague_area_code"))
    O_k = extract_travel_demands(employed_trip)
    #print(sum(O_k.values()))
    pi_kk, _ = context.stage("preprocess.clean_commute_prob")
    pi_k = pi_kk.groupby('home_zone_id')


    f_kk = pd.DataFrame(columns=["origin", "destination", "trip_count"])
    unique_zones = pi_k.commute_zone_id.unique()
    print(pi_kk.head())
    for o_k in O_k.keys:
        pi_k = pd.DataFrame(pi_kk[pi_kk.home_zone_id == o_k])
        f_kk.append(extract_trip_counts(O_k[o_k], pi_k))

    
    
    #print(df_workplaces.head())

    return
    #load activity chains to df matched
    #df_persons['employed'] = df_persons['employed'].map({1 : "yes", 2 : "yes", 
    #                                                 3 : "yes", 4 : "no", 5 : "no", 
    #                                                 6 : "no", 7 : "no", 8 : "student"})
    #df_persons.loc[(~(df_persons["studying"]== 1)) & (df_persons['employed']=='no'), "employed"] = "student"