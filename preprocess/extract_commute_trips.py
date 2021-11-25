import numpy as np
import pandas as pd
import tqdm


def configure(context):
    context.config("prague_area_code")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
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
    #no_trip = employed[~employed.hdm_source_id.isin(hts_workers)]
    #print("Employed with no HTS trip:", len(no_trip))
    employed_trip = employed[employed.hdm_source_id.isin(hts_workers)]
    return employed_trip

def extract_travel_demands(employed_trip):
    O_k = {}
    employed_zones = employed_trip.groupby("zone_id") #bydliste
    #print("Workers in zones (#)",len(employed_zones))
    for i, zone in employed_zones:
        O_k[i] = len(zone)
    return O_k

    
def extract_trip_counts(k, demand_k, pi_k, other_dests):
    f_k = pd.DataFrame()

    counts = list(np.random.multinomial(demand_k, pi_k.probability))
    counts.extend([0]*len(other_dests))

    dests = list(pi_k.commute_zone_id)
    dests.extend(other_dests)
    
    f_k["destination"] = dests
    f_k["trip_count"] = counts
    f_k["origin"] = k

    return f_k

def execute(context):
    df_matched = context.stage("synthesis.population.matched")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    df_workplaces, df_schools, df_shops, df_leisure = context.stage("preprocess.extract_amenities")
    pi_kk, _ = context.stage("preprocess.clean_commute_prob")
    pi_k = pi_kk.groupby('home_zone_id')


    for col in ["district_id", "zone_id"]:
        df_workplaces[col] = df_workplaces[col].fillna(-1).astype(int)
        print("Missing",col, len(df_workplaces.iloc[np.where(df_workplaces[col] == -1)]))


    #Assigning primary location (work): Step 1
    #extract employed people who travel to work
    employed_trip = extract_travelling_workers(df_trips, df_matched, context.config("prague_area_code"))
    

    #extract travel demands for each zone
    O_k = extract_travel_demands(employed_trip)
    
    #extract outgoing trip counts for each zone
    f_kk = pd.DataFrame(columns=["origin", "destination", "trip_count"])
    unique_zones = list(pi_kk.commute_zone_id.unique())

    for k in pi_kk.home_zone_id.unique(): # too slow
        pi_k = pi_kk[pi_kk.home_zone_id == k]
        other_ids = [ u for u in unique_zones if not u in list(pi_k.commute_zone_id)]
        
        f_kk = f_kk.append(extract_trip_counts(k, O_k[k], pi_k, other_ids))

    #validate trip counts
    #f_k = f_kk.groupby("origin")
    #for i,f in f_k:
    #    print(i, O_k[i], f.trip_count.sum())


    #remove trips that end outside Prague
    # TODO remove in the future
    trips_outside = f_kk[f_kk.destination == 999].index
    f_kk.drop(trips_outside, axis=0, inplace=True)
    #print(f_kk[f_kk.destination == 999].shape)

    return f_kk, employed_trip.person_id