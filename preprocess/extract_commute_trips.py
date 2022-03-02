import numpy as np
import pandas as pd
import tqdm


def configure(context):
    context.config("prague_area_code")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")
    context.stage("preprocess.extract_amenities")
    context.stage("preprocess.clean_commute_prob")
    context.stage("preprocess.extract_hts_trip_chains")
    context.stage("preprocess.zones")

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

def extract_school_distances(df_people, df_trips):
    print("Extracting school commute distances:")
    print("\t All trips:", df_trips.shape[0])

    print("\tCensus students traveling:",df_people.shape[0])
    school_trips = df_trips.loc[df_trips.traveler_id.isin(df_people.hdm_source_id.unique())]
    school_trips = school_trips.drop_duplicates(["traveler_id"])
    print("\tSchool (hts) trips to impute:", school_trips.shape[0])
    df_people = df_people.merge(school_trips[["traveler_id","beeline"]], 
                    left_on="hdm_source_id", right_on="traveler_id")
    print("\tCensus students with hts beeline:",df_people.shape[0])
    return df_people


def extract_travelling_workers(df_trips, df_matched, prague_area):
    #traveler ids with work trips in Prague
    work_trips = df_trips[df_trips.destination_purpose == "work"].copy()
    drop_area_codes = work_trips[work_trips.destination_code != prague_area].index.to_list() + work_trips[work_trips.origin_code != prague_area].index.to_list()

    valid_trips = work_trips.drop(drop_area_codes)
    hts_workers = valid_trips.traveler_id.unique()

    print("Workers in HTS trips:",len(hts_workers))
    print(len(df_trips.traveler_id.unique()))
    #print(df_matched.head())
    employed = df_matched.loc[df_matched.employment == "employed"]
    print("Employed in census:", len(employed))
    print("Employed (unique HTS ids) in census:", len(employed.hdm_source_id.unique()))

    no_trip = employed[~employed.hdm_source_id.isin(hts_workers)]
    print("Employed with no HTS trip:", len(no_trip))

    employed_trip = employed[employed.hdm_source_id.isin(hts_workers)]
    print("Employed (traveling) people in zones:",employed_trip.shape[0])
    print ("Employed people in zones")
    return employed_trip

def extract_travelling_students(df_trips, df_matched, prague_area):
     #traveler ids with work trips in Prague
    edu_trips = df_trips[df_trips.destination_purpose == "education"].copy()
    drop_area_codes = edu_trips[edu_trips.destination_code != prague_area].index.to_list() + edu_trips[edu_trips.origin_code != prague_area].index.to_list()

    valid_trips = edu_trips.drop(drop_area_codes)
    hts_students = valid_trips.traveler_id.unique()

    print("Students in HTS trips:",len(hts_students))
    #print(df_matched.head())
    students = df_matched.loc[df_matched.employment == "student"]
    #print("Students in census:", len(students))

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

    _, _, df_trips = context.stage("preprocess.clean_travel_survey")

    df_workplaces, df_schools, df_shops, df_leisure, df_other = context.stage("preprocess.extract_amenities")
    pi_kk, pi_kk_edu = context.stage("preprocess.clean_commute_prob")

    df_zones = context.stage("preprocess.zones")
    
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
    #employed_trip = extract_work_distances(employed_trip, df_trips)
    employed_trip = extract_primary_activity_distances(employed_trip, df_trips)

    #extract student who travel to school
    students_trip = extract_travelling_students(df_trips, df_matched, context.config("prague_area_code"))
    #students_trip = extract_school_distances(students_trip, df_trips)
    students_trip = extract_primary_activity_distances(students_trip, df_trips)

    print("Students/Workers overlap:", len(set(employed_trip.person_id.values).intersection(set(students_trip.person_id.values))) ,"people in census")

    #extract travel demands for each zone
    O_k_work = extract_travel_demands(employed_trip)
    O_k_edu = extract_travel_demands(students_trip)
    print("Extracted primary activity travel demands for traveling census people")
    
    
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
        #print(k)
        if(k in O_k_edu.keys()):
            f_kk_edu = f_kk_edu.append(extract_trip_counts(k, O_k_edu[k], pi_k, other_ids))
        else:
            f_kk_edu = f_kk_edu.append(extract_trip_counts(k, 0, pi_k, other_ids))


    print("Extracted trip counts for each zone.")

    print("Zones in edu Ok",len(list(O_k_edu.keys())))
    print("Zones in edu probs:",len(pi_kk_edu.home_zone_id.unique()))
    print("Zones in f_kk edu:",len(f_kk_edu.origin.unique()))

    df_students_nok = students_trip[~students_trip.zone_id.isin(pi_kk_edu.home_zone_id.unique())]
    students_trip = students_trip[students_trip.zone_id.isin(pi_kk_edu.home_zone_id.unique())]
    print("NOK students from zones we don't have probabilities for:")
    print(df_students_nok.describe())
    print(df_zones[df_zones.zone_id.isin(df_students_nok.zone_id.unique())][["zone_id", "district_name"]])
    
    #remove trips that end outside Prague
    # TODO remove in the future
    trips_outside = f_kk_work[f_kk_work.destination == 999]
    print("Work trips leading outside Prague area:",trips_outside.trip_count.sum())
    #f_kk = f_kk[f_kk.destination != 999]
    #print(f_kk[f_kk.destination == 999].shape)
    print("All abstract WORK trips in Prague:", f_kk_work.trip_count.sum())
    print("All abstract SCHOOL trips in Prague:", f_kk_edu.trip_count.sum())
    print("Travel demand WORK:",sum(list(O_k_work.values())))
    print("Travel demand EDUCATION:",sum(list(O_k_edu.values())))

    return f_kk_work, employed_trip, f_kk_edu, students_trip