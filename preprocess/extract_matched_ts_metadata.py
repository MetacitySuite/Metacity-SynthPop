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

    

def extract_trip_counts(k, demand_k, pi_k, other_dests):
    f_k = pd.DataFrame()#columns=["origin", "destination", "trip_count"])

    counts = list(np.random.multinomial(demand_k, pi_k.probability))
    counts.extend([0]*len(other_dests))

    dests = list(pi_k.commute_zone_id)
    dests.extend(other_dests)
    f_k["destination"] = dests
    f_k["trip_count"] = counts
    f_k["origin"] = k
    #print(f_k.head())
    return f_k



def execute(context):
    df_zones = context.stage("preprocess.zones")
    df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_workplaces, df_schools, df_shops, df_leisure = context.stage("preprocess.extract_amenities")
    for col in ["district_id", "zone_id"]:
        df_workplaces[col] = df_workplaces[col].fillna(-1).astype(int)
        print("Missing",col, len(df_workplaces.iloc[np.where(df_workplaces[col] == -1)]))


    employed_trip = extract_travelling_workers(df_trips, df_matched, context.config("prague_area_code"))
    O_k = extract_travel_demands(employed_trip)
    #print(sum(O_k.values()))
    pi_kk, _ = context.stage("preprocess.clean_commute_prob")
    pi_k = pi_kk.groupby('home_zone_id')


    f_kk = pd.DataFrame(columns=["origin", "destination", "trip_count"])
    unique_zones = pi_kk.commute_zone_id.unique()

    for k in pi_kk.home_zone_id.unique():
        pi_k = pi_kk[pi_kk.home_zone_id == k]
        other_ids = [ u for u in unique_zones if not u in list(pi_k.commute_zone_id)]
        f_kk = f_kk.append(extract_trip_counts(k, O_k[k], pi_k, other_ids))
    
    print(f_kk.head())

    #validate trip counts
    #f_k = f_kk.groupby("origin")
    #for i,f in f_k:
    #    print(i, O_k[i], f.trip_count.sum())

    #sample destination candidates with replacement for each f_kk in 
    C_kk = set()



    return