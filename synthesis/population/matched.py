import numpy as np
import pandas as pd
import synthesis.algorithms.hot_deck_matching


def correct_string(s):
    if("Praha" not in s):
        s = "Praha-" + s
    return s

def remap_districts(context, df):
    df_district_mapping = pd.read_csv(context.config("data_path") + context.config("district_mapping_file"), delimiter=";")
    df_district_mapping.columns = ['district_name', 'mapped_district_name']
    df_district_mapping['district_name'] = df_district_mapping['district_name'].apply(lambda x: correct_string(x))
    df_district_mapping['mapped_district_name'] = df_district_mapping['mapped_district_name'].apply(lambda x: correct_string(x))
    df = df.merge(df_district_mapping, on='district_name')
    df = df.drop(['district_name', "zone_centroid"], axis = 1)
    df.columns = ['person_id', 'zone_id', 'sex', 'age', 'employment', 'age_class', 'district_name']
    return df

def set_students(row):
    if(row.age >= 6 and row.age < 15):
        return "student"
    
    return row.employment

def remove_invalid_chains(df_matched, hts_activities):

    start_outside_home = set(hts_activities[hts_activities.activity_order == 0][hts_activities.purpose != "home"].traveler_id.values)
    print("HTS people starting outside home:",len(start_outside_home))
    
    
    end_outside_home = set()
    
    for i,traveler in hts_activities.groupby("traveler_id"):
        max_order = max(traveler.activity_order.values)
        #print(max_order)
        if(traveler[traveler.activity_order == max_order].purpose.values != "home" or traveler[traveler.activity_order == 0].purpose.values != "home"):
            print(traveler)
            end_outside_home.add(i)

    print("HTS people ending outside home:",len(set(end_outside_home)))
    # Filter matched
    invalid_hts_travelers = start_outside_home.union(end_outside_home)
    df_valid = df_matched[~df_matched.hdm_source_id.isin(invalid_hts_travelers)]
    print("Removed:", df_matched.shape[0] - df_valid.shape[0])
    df_valid.reset_index(inplace=True)
    return df_valid

def configure(context):
    #context.stage("synthesis.population.sampled")
    context.config("data_path")
    context.config("district_mapping_file") #data sensitive
    context.config("matching_processes")
    context.config("matching_minimum_samples")
    context.stage("preprocess.clean_census")
    context.stage("preprocess.clean_travel_survey")
    context.stage("preprocess.extract_hts_trip_chains")
    context.stage("preprocess.zones")

def execute(context):
    #df_census = context.stage("synthesis.population.sampled")
    df_zones = context.stage("preprocess.zones")
    df_census = context.stage("preprocess.clean_census")
    df_households, df_travelers, _ = context.stage("preprocess.clean_travel_survey")

    #set source and target for statistical matching
    df_source = df_travelers.drop(['driving_license', 'car_avail', 'bike_avail', 'pt_avail'], axis = 1)
    df_target = df_census

    #set age class for better matching
    AGE_BOUNDARIES = [18, 30, 45, 65, 75, np.inf]
    df_source["age_class"] = np.digitize(df_source["age"], AGE_BOUNDARIES, right = True)
    df_target["age_class"] = np.digitize(df_target["age"], AGE_BOUNDARIES, right = True)

    # group employment to fewer categories
    employment_map = {'employee, employer, self-employed, or helping' : "employed", 
                        'working retiree' : "employed",
                        'non-working retiree' : "unemployed", 
                        'pupils, students, apprentices' : "student", 
                        'maternity leave' : "unemployed",  
                        'with own source of living' : "employed",
                        'person in household, pre-school child, other dependents' : "unemployed", 
                        'other unemployed' : "unemployed", 
                        'unemployed seeking first employment' : "unemployed", 
                        'working students and apprentices' : "student"
                        }

    df_source.employment = df_source.employment.map(employment_map)
    df_target.employment = df_target.employment.map(employment_map)
    #all kids aged 6-14 are set as students
    #TODO
    print(df_target.employment.value_counts())
    df_target.employment = df_target.apply(lambda row: set_students(row), axis=1)
    print(df_target.employment.value_counts())

    #add district_name column for pairing
    df_source = df_source.merge(df_households, on='household_id')
    df_source = df_source.drop(['persons_number', 'car_number', 'bike_number'], axis = 1)
    df_target = df_target.merge(df_zones, on='zone_id')
    df_target = df_target.drop(['geometry', 'district_id'], axis = 1)

    
    #set districts in census by district mapping file - data sensitive edit (due to missing district representation in the travel survey)
    df_target = remap_districts(context, df_target)

    #save to CSV
    df_source.to_csv(context.config("output_path")+"/clean_hts_matched.csv")
    df_target.to_csv(context.config("output_path")+"/clean_census_matched.csv")
    
    
    synthesis.algorithms.hot_deck_matching.run(
        df_target, df_source,
        "traveler_id",
        ["age_class", "sex", "employment"],
        ['district_name'],
        minimum_source_samples = context.config("matching_minimum_samples"),
        process_num = context.config("matching_processes")
    )

    #remove non-matched people from census
    unmatched = df_target.loc[df_target['hdm_source_id'] == -1]
    print("Unmatched (#) in census:",len(unmatched))
    print(unmatched.employment.value_counts())
    df_target.drop(unmatched.index, axis=0, inplace=True)
    df_target.reset_index(inplace=True)
    #print(len(df_target.loc[df_target['hdm_source_id'] == -1]))
    #return matched census individuals

    hts_activities, _ = context.stage("preprocess.extract_hts_trip_chains")
    #TODO export data pro vojtu

    df_matched = remove_invalid_chains(df_target, hts_activities)
    return df_matched