import pandas as pd
import numpy as np
import re

def age_class_to_interval(age_class):
    bounds = re.split("[-|+]", age_class)
    bounds = [np.inf if x == '' else int(x) for x in bounds]
    return pd.Interval(bounds[0], bounds[1], closed='left')

def clean_household_data(df):
    #cars = every type of a car and motorcycles
    car_list = ['car_private', 'car_company', 'car_util', 'car_other']
    df['car_number'] = df[car_list].sum(axis=1)
    df = df.drop(car_list, axis=1)
    df = df[['household_id', 'district', 'persons_number', 'car_number', 'bike_number']]
    return df

def clean_traveler_data(df):
    #drop rows with incomplete data
    df = df.dropna(subset = ['sex', 'employment', 'work_status', 'car_avail'])

    employed_list_cz = ['zaměstnanec, zaměstnavatel, samostatně činný či pomáhající', 'pracující důchodce']
    employed_list = ['employee, employer, self-employed, or helping', 'working retiree']
    student_list_cz = ['žák ZŠ', 'pracující SŠ student nebo učeň','student SŠ', 'student VŠ', 'pracující VŠ student']
    student_list = ['elementary school pupil', 'working high school student or apprentice', 'high school student', 'university student', 'working university student' ]

    #add missing values
    df['age'] = df['age'].fillna(df.groupby(['sex', 'employment'])['age'].transform('median')).astype(int)

    #remap values
    df['sex'] = df['sex'].replace(['muž', 'žena'], ['M', 'F'])
    df['trip_today'] = df['trip_today'].replace(['ano', 'ne'], [True, False])
    df['car_avail'] = df['car_avail'].replace(['ano', 'ne'], [True, False])
    df['bike_avail'] = df['bike_avail'].replace(['ano', 'ne'], [True, False])
    df['pt_avail'] = df['pt_avail'].replace(['ano', 'ne'], [True, False])
    df['work_status'] = df['work_status'].replace(['ano', 'ne'], ['yes', 'no'])
    df['employment'] = df['employment'].replace(employed_list_cz + student_list_cz, employed_list + student_list)
    
    #add student as new work status
    df.loc[df['employment'].isin(student_list), 'work_status'] = "student"

    #remap original column (driving_license_none)
    df['driving_license'] = df['driving_license'].replace(['ano', 'ne'], [False, True])

    #fill NA - use sociodemographics
    df.loc[(df['age'] >= 18) & (df['driving_license'].isna()) , 'driving_license'] = True
    df.loc[(df['age'] < 18) & (df['driving_license'].isna()) , 'driving_license'] = False
    df.loc[(df['age'] < 16) & (df['age'] > 64), 'pt_avail'] = True

    #finally drop NA in pt_avail
    df = df.dropna(subset = ['pt_avail'])
    #print(df.loc[(df['employment'].isin(employed_list_cz)) & (df['work_status'] == 'no'), ['employment', 'work_status']])
    return df

def clean_activity_chain(df):
    primary = ['home', 'work', 'education']
    #remove travelers who do not have any destination at home
    travelers_with_home = df.loc[ (df['origin_purpose'] == 'home') | (df['destination_purpose'] == 'home') ]['traveler_id'].unique()
    df = df.loc[df['traveler_id'].isin(travelers_with_home)]

    #check if travelers have meaningful connected trip order
    df_trips_count = df.groupby('traveler_id').size().reset_index(name='trips')
    deleted_travelers = []
    for i, row in df_trips_count.iterrows():
        traveler_id = row['traveler_id']
        trip_count = row['trips']
        df_daily_plan = df.loc[(df['traveler_id'] == traveler_id)]
    
        #check if trips are connected
        for order in range(1, trip_count):
            prev_trip = df_daily_plan.loc[(df_daily_plan['trip_order'] == (order))]
            next_trip = df_daily_plan.loc[(df_daily_plan['trip_order'] == (order + 1))]
            
            #missing trips completely
            if (prev_trip.empty or next_trip.empty): 
                deleted_travelers.append(traveler_id)
                break
        
            #trips are not connected
            prev_destination_purpose = prev_trip['destination_purpose'].values[0]
            next_origin_purpose = next_trip['origin_purpose'].values[0]
        
            if(prev_destination_purpose != next_origin_purpose):
                deleted_travelers.append(traveler_id)
                break

    df = df.loc[~df['traveler_id'].isin(deleted_travelers)]

    ##TODO: add dummy trip to matsim XML: end time = arrival time, same coordinates, change activity
    ##agents start and end the day with same type activity (does not need same place)
    #first_origin = df_daily_plan.loc[(df_daily_plan['trip_order'] == 1)]['origin_purpose'].values[0]
    #last_destination = df_daily_plan.loc[(df_daily_plan['trip_order'] == trip_count)]['destination_purpose'].values[0]

    #if(first_origin != last_destination):
    #    ...

    return df

def clean_trip_data(df, inside_area_only = True, area_code = 1000):
    #remap values
    purpose_list_cz = ['bydliště', 'práce', 'vzdělávání', 'nakupování', 'volno', 'ostatní', 'zařizování', 'prac. cesta', 'stravování']
    purpose_list = ['home', 'work', 'education', 'shop', 'leisure', 'other', 'other', 'other', 'shop']
    mode_list_cz = ['auto-d', 'auto-p', 'bus', 'kolo', 'MHD', 'ostatní', 'pěšky', 'vlak']
    mode_list = ['car', 'car-passenger', 'pt', 'bike', 'pt', 'other', 'walk', 'pt']

    df['origin_purpose'] = df['origin_purpose'].replace(purpose_list_cz, purpose_list)
    df['destination_purpose'] = df['destination_purpose'].replace(purpose_list_cz, purpose_list)
    df['last_trip'] = df['last_trip'].replace(['ano', 'ne'], [True, False])
    df['traveling_mode'] = df['traveling_mode'].replace(mode_list_cz, mode_list)

    #drop unidentified travelers
    df = df.dropna(subset=['traveler_id'])
    df['traveler_id'] = df['traveler_id'].astype(int)

    #drop travelers with missing mandatory columns
    removed_travelers = df.loc[ (df['departure_h'].isna()) | (df['departure_m'].isna()) |
                                (df['arrival_h'].isna()) | (df['arrival_m'].isna()) |
                                (df['origin_purpose'].isna()) | (df['destination_purpose'].isna()) |
                                (df['traveling_mode'].isna())]['traveler_id'].unique()
    df = df.loc[~df['traveler_id'].isin(removed_travelers)]

    #expect trips to start and end in the same area code
    df['origin_code'] = df['origin_code'].fillna(df['destination_code'])
    df['destination_code'] = df['destination_code'].fillna(df['origin_code'])
    df['origin_code'] = df['origin_code'].fillna(-1)
    df['destination_code'] = df['destination_code'].fillna(-1)
    df['origin_code'] = df['origin_code'].astype(int)
    df['destination_code'] = df['destination_code'].astype(int)

    #remove travelers that leave or enter Prague during the day
    if(inside_area_only):
        removed_travelers = df.loc[(df['origin_code'] != area_code) | (df['destination_code'] != area_code)]['traveler_id'].unique()
        df = df.loc[~df['traveler_id'].isin(removed_travelers)]
    else: #TODO
        ...

    #drop columns not useful anymore
    df = df.drop(['origin_code', 'destination_code'], axis=1)

    #clean and connect activity chains, purge nonsense
    df = clean_activity_chain(df)
    return df

def connect_tables(df_hh, df_travelers, df_trips):
    travelers_with_trips = df_trips['traveler_id'].unique()
    travelers_with_trips_count = len(travelers_with_trips)
    
    #fill NA, drop travelers with NA where no trips are specified
    travelers_wrong_na_mask = (df_travelers['trip_today'].isna()) & (df_travelers['traveler_id'].isin(travelers_with_trips))
    df_travelers.loc[travelers_wrong_na_mask, 'trip_today'] = True
    #drop the rest
    df_travelers = df_travelers.dropna(subset = ['trip_today']) 

    #non-traveling people should have a False flag and no trips
    non_travelers = df_travelers.loc[df_travelers['trip_today'] == False, 'traveler_id'].unique()

    #clear travelers with removed trips happening outside the area or missing trips completely
    valid_travelers = df_travelers.loc[(df_travelers['traveler_id'].isin(travelers_with_trips)) | (df_travelers['traveler_id'].isin(non_travelers)), 'traveler_id'].unique()
    df_travelers = df_travelers.loc[df_travelers['traveler_id'].isin(valid_travelers)]
    
    #trips need to have traveler data
    travelers = df_travelers['traveler_id'].unique()
    df_trips = df_trips.loc[df_trips['traveler_id'].isin(travelers)]

    #clean unused households
    households = df_travelers['household_id'].unique()
    df_hh = df_hh.loc[df_hh['household_id'].isin(households)]

    #remap ids
    old_traveler_id = df_travelers['traveler_id'].unique()
    new_traveler_id = np.arange(len(old_traveler_id))
    mapping_traveler_id = dict(zip(old_traveler_id, new_traveler_id))
    
    df_travelers['traveler_id'] = df_travelers['traveler_id'].replace(mapping_traveler_id)
    df_trips['traveler_id'] = df_trips['traveler_id'].replace(mapping_traveler_id)
    
    old_household_id = df_hh['household_id'].unique()
    new_household_id = np.arange(len(old_household_id))
    mapping_household_id = dict(zip(old_household_id, new_household_id))
    
    df_hh['household_id'] = df_hh['household_id'].replace(mapping_household_id)
    df_travelers['household_id'] = df_travelers['household_id'].replace(mapping_household_id)

    return df_hh, df_travelers, df_trips


def configure(context):
    context.config("data_path")
    context.config("travel_survey_files")

def execute(context):
    #read CSV data
    df_hh = pd.read_csv(context.config("data_path") + context.config("travel_survey_files") + "H.csv", encoding='cp1250', delimiter=";")
    df_persons = pd.read_csv(context.config("data_path") + context.config("travel_survey_files") + "P.csv", encoding='cp1250', delimiter=";")
    df_trips = pd.read_csv(context.config("data_path") + context.config("travel_survey_files") + "T.csv", encoding='cp1250', delimiter=";")

    #filter and rename columns
    df_hh = df_hh[['H_ID', 'NAZ_MOaMC', 'H_persons', 'H_venr_car_private', 'H_venr_car_company', 'H_venr_util', 'H_venr_other','H_venr_bike']]
    df_hh.columns = ['household_id', 'district', 'persons_number', 'car_private', 'car_company', 'car_util', 'car_other', 'bike_number']


    df_travelers = df_persons[['P_ID', 'H_ID', 'P_gender', 'P_age', 
                            'P_work', 'P_work_status', 
                            'P_driving_licence_none', 'P_triptoday', 
                            'P_caravail', 'P_bikeavail', 'P_ptavail' ]]

    df_travelers.columns = ['traveler_id', 'household_id', 'sex', 'age', 
                            'employment', 'work_status', 
                            'driving_license', 'trip_today', 
                            'car_avail', 'bike_avail', 'pt_avail']

    df_trips = df_trips[['P_ID', 'T_ord', 
                        'T_O_time_hh', 'T_O_time_min', 'T_D_time_hh', 'T_D_time_min',
                        'T_O_purpose', 'T_D_purpose', 'T_last_trip',
                        'T_dist_dir','T_mainmode', 'T_O_orp_code', 'T_D_orp_code']]
    
    df_trips.columns = ['traveler_id', 'trip_order', 
                        'departure_h', 'departure_m', 'arrival_h', 'arrival_m',
                        'origin_purpose', 'destination_purpose', 'last_trip',
                        'beeline', 'traveling_mode', 'origin_code', 'destination_code']

    df_hh = clean_household_data(df_hh)
    df_travelers = clean_traveler_data(df_travelers)
    df_trips = clean_trip_data(df_trips)

    #re-connect all data
    df_hh, df_travelers, df_trips = connect_tables(df_hh, df_travelers, df_trips)

    #df_hh.to_csv(r'/home/metakocour/Projects/Metacity-SynthPop/output/H-1.csv', index = False, sep=';')
    #df_travelers.to_csv(r'/home/metakocour/Projects/Metacity-SynthPop/output/P-1.csv', index = False, sep=';')
    #df_trips.to_csv(r'/home/metakocour/Projects/Metacity-SynthPop/output/T-1.csv', index = False, sep=';')
    return df_hh, df_travelers, df_trips