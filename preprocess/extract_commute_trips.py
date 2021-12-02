import numpy as np
import pandas as pd
import tqdm


def configure(context):
    context.config("prague_area_code")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
    context.stage("preprocess.extract_amenities")
    context.stage("preprocess.clean_commute_prob")



def extract_work_distances(df_people, df_trips):
    print("Extracting commute distances:")
    print("\tAll trips:", df_trips.shape[0])
    print("\tTrips with missing beeline:", len(df_trips[df_trips.beeline.isna()]))
    work_trips = df_trips[df_trips.destination_purpose == "work"]
    work_trips = work_trips[work_trips.origin_purpose == "home"]

    #remove trips that end outside prague which would skew the beeline data
    work_trips = work_trips[work_trips.destination_code != 999]
    print("\tWork trips:", len(work_trips))

    #fill in beeline - use origin, destination code (27perc missing)
    beeline_median = work_trips.groupby(['origin_code', 'destination_code'])['beeline'].transform('median')
    df_trips.beeline = df_trips['beeline'].fillna(value=beeline_median)
    print("\tTrips with missing beeline:", len(df_trips[df_trips.beeline.isna()]))
    #fill rest with median
    df_trips.beeline = df_trips['beeline'].fillna(value=work_trips['beeline'].median())
    print("\tTrips with missing beeline:", len(df_trips[df_trips.beeline.isna()]))
    
    print("\tCensus people to fill with beeline:",df_people.shape[0])
    work_trips = df_trips.loc[df_trips.traveler_id.isin(df_people.hdm_source_id.unique())]
    work_trips = work_trips.drop_duplicates(["traveler_id"])
    print("\tWork trips to impute:", work_trips.shape[0])
    df_people = df_people.merge(work_trips[["traveler_id","beeline"]], 
                    left_on="hdm_source_id", right_on="traveler_id")
    print("\tCensus workers with beeline:",df_people.shape[0])
    return df_people

def extract_school_distances(df_people, df_trips):
    print("Extracting commute distances:")
    print("\tAll trips:", df_trips.shape[0])
    print("\tTrips with missing beeline:", len(df_trips[df_trips.beeline.isna()]))
    school_trips = df_trips[df_trips.destination_purpose == "education"]
    school_trips = school_trips[school_trips.origin_purpose == "home"]

    #remove trips that end outside prague which would skew the beeline data
    school_trips = school_trips[school_trips.destination_code != 999]
    print("\tSchool trips:", len(school_trips))

    #fill in beeline - use origin, destination code (27perc missing)
    beeline_median = school_trips.groupby(['origin_code', 'destination_code'])['beeline'].transform('median')
    df_trips.beeline = df_trips['beeline'].fillna(value=beeline_median)
    print("\tTrips with missing beeline:", len(df_trips[df_trips.beeline.isna()]))
    #fill rest with median
    df_trips.beeline = df_trips['beeline'].fillna(value=school_trips['beeline'].median())
    print("\tTrips with missing beeline:", len(df_trips[df_trips.beeline.isna()]))
    
    print("\tCensus people to fill with beeline:",df_people.shape[0])
    school_trips = df_trips.loc[df_trips.traveler_id.isin(df_people.hdm_source_id.unique())]
    school_trips = school_trips.drop_duplicates(["traveler_id"])
    print("\tSchool trips to impute:", school_trips.shape[0])
    df_people = df_people.merge(school_trips[["traveler_id","beeline"]], 
                    left_on="hdm_source_id", right_on="traveler_id")
    print("\tCensus students with beeline:",df_people.shape[0])
    return df_people


def extract_travelling_workers(df_trips, df_matched, prague_area):
     #traveler ids with work trips in Prague
    hts_workers = df_trips.iloc[np.where(df_trips.destination_purpose == "work") 
                            and np.where(df_trips.destination_code == prague_area) 
                            and np.where(df_trips.origin_code == prague_area) ].traveler_id.unique()

    print("Workers in HTS:",len(hts_workers))
    #print(df_matched.head())
    employed = df_matched.loc[df_matched.employment == "employed"]
    print("Employed in census:", len(employed))

    no_trip = employed[~employed.hdm_source_id.isin(hts_workers)]
    print("Employed with no HTS trip:", len(no_trip))

    employed_trip = employed[employed.hdm_source_id.isin(hts_workers)]
    print("Employed (traveling) people in zones:",employed_trip.shape[0])
    return employed_trip

def extract_travelling_students(df_trips, df_matched, prague_area):
     #traveler ids with work trips in Prague
    hts_students= df_trips.iloc[np.where(df_trips.destination_purpose == "education") 
                            and np.where(df_trips.destination_code == prague_area) 
                            and np.where(df_trips.origin_code == prague_area) ].traveler_id.unique()

    print("Students in HTS:",len(hts_students))
    #print(df_matched.head())
    students = df_matched.loc[df_matched.employment == "student"]
    print("Students in census:", len(students))

    no_trip = students[~students.hdm_source_id.isin(hts_students)]
    print("Students with no HTS trip:", len(no_trip))

    students_trip = students[students.hdm_source_id.isin(hts_students)]
    print("Studying (traveling) people in zones:",students_trip.shape[0])
    return students_trip

def extract_travel_demands(employed_trip):
    O_k = {}
    employed_zones = employed_trip.groupby("zone_id") #bydliste
    #print("Workers in zones (#)",len(employed_trip))
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
    pi_kk, pi_kk_edu = context.stage("preprocess.clean_commute_prob")
    
    print("matched:",df_matched.shape[0])
    print("df_trips", df_trips.shape[0])


    print(df_workplaces.shape[0])
    for col in ["district_id", "zone_id"]:
        df_workplaces[col] = df_workplaces[col].fillna(-1).astype(int)
        print("Workplaces missing in ",col, len(df_workplaces.iloc[np.where(df_workplaces[col] == -1)]))

    print(df_schools.shape[0])
    for col in ["district_id", "zone_id"]:
        df_schools[col] = df_schools[col].fillna(-1).astype(int)
        print("Workplaces missing in ",col, len(df_schools.iloc[np.where(df_schools[col] == -1)]))


    #Assigning primary location (work): Step 1
    #extract employed people who travel to work
    employed_trip = extract_travelling_workers(df_trips, df_matched, context.config("prague_area_code"))
    employed_trip = extract_work_distances(employed_trip, df_trips)

    #extract student who travel to school
    students_trip = extract_travelling_students(df_trips, df_matched, context.config("prague_area_code"))
    students_trip = extract_school_distances(students_trip, df_trips)

    #extract travel demands for each zone
    O_k_work = extract_travel_demands(employed_trip)
    O_k_edu = extract_travel_demands(students_trip)
    print("Travel demand WORK:",sum(list(O_k_work.values())))
    print("Travel demand EDUCATION:",sum(list(O_k_edu.values())))
    
    #extract outgoing trip counts for each zone
    f_kk_work = pd.DataFrame(columns=["origin", "destination", "trip_count"])
    unique_zones = list(pi_kk.commute_zone_id.unique())

    for k in pi_kk.home_zone_id.unique(): # too slow
        pi_k = pi_kk[pi_kk.home_zone_id == k]
        other_ids = [ u for u in unique_zones if not u in list(pi_k.commute_zone_id)]
        
        f_kk_work = f_kk_work.append(extract_trip_counts(k, O_k_work[k], pi_k, other_ids))

    f_kk_edu = pd.DataFrame(columns=["origin", "destination", "trip_count"])
    unique_zones = list(pi_kk_edu.commute_zone_id.unique())

    for k in pi_kk_edu.home_zone_id.unique(): # too slow
        pi_k = pi_kk_edu[pi_kk_edu.home_zone_id == k]
        other_ids = [ u for u in unique_zones if not u in list(pi_k.commute_zone_id)]
        
        f_kk_edu = f_kk_edu.append(extract_trip_counts(k, O_k_edu[k], pi_k, other_ids))

    #validate trip counts
    #f_k = f_kk.groupby("origin")
    #for i,f in f_k:
    #    print(i, O_k[i], f.trip_count.sum())
    
    #remove trips that end outside Prague
    # TODO remove in the future
    trips_outside = f_kk_work[f_kk_work.destination == 999]
    print("Work trips leading outside Prague area:",trips_outside.trip_count.sum())
    #f_kk = f_kk[f_kk.destination != 999]
    #print(f_kk[f_kk.destination == 999].shape)
    print("All abstract WORK trips in Prague:", f_kk_work.trip_count.sum())
    print("All abstract SCHOOL trips in Prague:", f_kk_edu.trip_count.sum())

    return f_kk_work, employed_trip