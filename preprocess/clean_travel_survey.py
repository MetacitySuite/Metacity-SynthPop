import pandas as pd
import numpy as np
import re
from tqdm import tqdm

"""
This stage cleans the travel survey. The travel survey consists of three tables: H (Households), P (Persons - travelers), T (Trips).
"""

##TODO: add dummy trip to matsim XML: end time = arrival time, same coordinates, change activity
##agents start and end the day with same type activity (does not need same place)
#first_origin = df_daily_plan.loc[(df_daily_plan['trip_order'] == 1)]['origin_purpose'].values[0]
#last_destination = df_daily_plan.loc[(df_daily_plan['trip_order'] == trip_count)]['destination_purpose'].values[0]

#if(first_origin != last_destination):
#    ...

#Suppress possible (false positive) warning - returning a view versus a copy.
#pd.options.mode.chained_assignment = None  # default='warn'
def fast_mode(df, key_cols, value_col):
    """ 
    Calculate a column mode, by group, ignoring null values. 

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame over which to calcualate the mode. 
    key_cols : list of str
        Columns to groupby for calculation of mode.
    value_col : str
        Column for which to calculate the mode. 

    Return
    ------ 
    pandas.DataFrame
        One row for the mode of value_col per key_cols group. If ties, 
        returns the one which is sorted first. 
    """
    return (df.groupby(key_cols + [value_col]).size() 
              .to_frame('counts').reset_index() 
              .sort_values('counts', ascending=False) 
              .drop_duplicates(subset=key_cols)).drop(columns='counts')


def age_class_to_interval(age_class):
    bounds = re.split("[-|+]", age_class)
    bounds = [np.inf if x == '' else int(x) for x in bounds]
    return pd.Interval(bounds[0], bounds[1], closed='left')

def clean_household_data(context, df):
    #cars = every type of a car and motorcycles
    car_list = ['car_private', 'car_company', 'car_util', 'car_other']
    df['car_number'] = df[car_list].sum(axis=1)
    df = df.drop(car_list, axis=1)
    df = df[['household_id', 'district_name', 'persons_number', 'car_number', 'bike_number']]
    return df

"""
Clean table P (Person).
"""

def clean_traveler_data(context, df):
    #drop rows with incomplete data
    df = df.dropna(subset = ['sex', 'employment'])
    
    #values for remapping
    _, ts_values_dict = context.stage("preprocess.coded_values")
    employment_list_cz = ts_values_dict["employed_list_cz"] + ts_values_dict["unemployed_list_cz"] + ts_values_dict["edu_list_cz"] 
    employment_list = ts_values_dict["employed_list"] + ts_values_dict["unemployed_list"] + ts_values_dict["edu_list"] 

    #add missing values
    df.loc[:, 'age'] = df['age'].fillna(df.groupby(['sex', 'employment'])['age'].transform('median')).astype(int)

    #remap values
    df.loc[:, 'sex'] = df['sex'].replace(['muž', 'žena'], ['M', 'F'])
    df.loc[:, 'trip_today'] = df['trip_today'].replace(['ano', 'ne', np.nan], [True, False, False])
    df.loc[:, 'car_avail'] = df['car_avail'].replace(['ano', 'ne'], [True, False])
    df.loc[:, 'bike_avail'] = df['bike_avail'].replace(['ano', 'ne'], [True, False])
    df.loc[:, 'pt_avail'] = df['pt_avail'].replace(['ano', 'ne'], [True, False])
    df.loc[:, 'employment'] = df['employment'].replace(employment_list_cz, employment_list)
    
    #remap original column (driving_license_none)
    df.loc[:, 'driving_license'] = df['driving_license'].replace(['ano', 'ne'], [False, True])

    #fill NA - use sociodemographics
    df.loc[(df['age'] >= 18) & (df['driving_license'].isna()) , 'driving_license'] = True
    df.loc[(df['age'] < 18) & (df['driving_license'].isna()) , 'driving_license'] = False
    df.loc[(df['age'] < 16) | (df['age'] > 64), 'pt_avail'] = True

    #finally drop NA in pt_avail
    #df = df.dropna(subset = ['pt_avail'])
    return df

"""
Validate activity chain. Remove travelers who do not have at least one "home" origin and/or destination.
Check if the trips done by a person are connected.
"""

def clean_activity_chain(df):
    primary = ['home', 'work', 'education']

    #check if travelers have meaningful connected trip order
    df_trips_count = df.groupby('traveler_id').size().reset_index(name='trips')
    deleted_travelers = []
    pbar = tqdm(df_trips_count.iterrows())
    for i, row in pbar:
        pbar.set_description("Checking trip connectivity for each traveler")
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
    return df

def calculate_row_percentage(df, column, value, sumcolumn = None):
    if(sumcolumn != None):
        count = df.loc[ (df[column] == value), sumcolumn].sum()
        total = df[sumcolumn].sum()
    else:
        count = len(df.loc[ (df[column] == value)])
        total = len(df)
    return count/total

def get_commute_trips(df, purpose):
    df_com = df.loc[ 
               ( (df['origin_purpose'] == 'home') &  (df['destination_purpose'].isin(purpose))   ) | 
               ( (df['origin_purpose'].isin(purpose)) & (df['destination_purpose'] == 'home')    ) ]
    return df_com

def fill_missing_area_code(context, df):
    area_code = context.config("prague_area_code")
    df_commute_work, df_commute_edu = context.stage("preprocess.clean_commute_prob")
    df_commute = pd.concat([df_commute_work, df_commute_edu])
    
    #outside_area_work_prob = calculate_row_percentage(df_commute_work, 'commute_zone_id', 999, 'person_number')
    #outside_area_education_prob = calculate_row_percentage(df_commute_edu, 'commute_zone_id', 999, 'person_number')
    #outside_area_prob = calculate_row_percentage(df_commute,'commute_zone_id', 999, 'person_number')

    df_c = df.loc[ (df['origin_code'].isna()) | (df['destination_code'].isna()) ]

    #df_c.to_csv(context.config("output_path") + "T-4b.csv", index = False, sep=';')
    
    #work commute - fill na with people staying in the area to preserve distribution according to commute probabilities
    na_index_work = get_commute_trips(df_c, ['work']).index
    df.loc[na_index_work, 'origin_code'] = df.loc[na_index_work, 'origin_code'].fillna(area_code)
    df.loc[na_index_work, 'destination_code'] = df.loc[na_index_work, 'destination_code'].fillna(area_code)

    #education commute - fill na with people staying in the area
    na_index_edu = get_commute_trips(df_c, ['education']).index
    df.loc[na_index_edu, 'origin_code'] = df.loc[na_index_edu, 'origin_code'].fillna(area_code)
    df.loc[na_index_edu, 'destination_code'] = df.loc[na_index_edu, 'destination_code'].fillna(area_code)
    return df

"""
Filter trips by area code - home purposes must be in given area
Commuters to work/education stay in area as well.
"""
def filter_trips_by_area(context, df):
    area_code = context.config("prague_area_code")

    #keep travelers who live in the area, delete NA as well (0.7% of data)
    live_inside_area = df.loc[ (df['origin_purpose'] == 'home') & (df['origin_code'] == area_code)]['traveler_id']
    live_inside_area.append = df.loc[ (df['destination_purpose'] == 'home') & (df['destination_code'] == area_code )]['traveler_id']
    df = df.loc[df['traveler_id'].isin(live_inside_area)]

    #fill in missing area for work and education
    df = fill_missing_area_code(context, df)

    return df

def calculate_time_in_seconds(df):
    df['departure_time'] = df['departure_h'] * 3600 + df['departure_m'] * 60
    df['arrival_time'] = df['arrival_h'] * 3600 + df['arrival_m'] * 60
    df = df.drop(['departure_h', 'departure_m', 'arrival_h', 'arrival_m'], axis = 1)
    return df

"""
Cleans table T (Trips). 
Removes travelers without ID, departure and/or arrival time, origin and/or destination, travelling mode.
Fills NA in origin and destination code based on either one of those (expect trips to start and end in the same area code).
For now remove travelers leaving and/or entering chosen area during the day.
Validates activity chain.
"""
def clean_trip_data(context, df):
    #remap values
    _, ts_values_dict = context.stage("preprocess.coded_values")
    df.loc[:, 'origin_purpose'] = df['origin_purpose'].replace(ts_values_dict["purpose_list_cz"], ts_values_dict["purpose_list"])
    df.loc[:, 'destination_purpose'] = df['destination_purpose'].replace(ts_values_dict["purpose_list_cz"], ts_values_dict["purpose_list"])
    df.loc[:, 'last_trip'] = df['last_trip'].replace(['ano', 'ne'], [True, False])
    df.loc[:, 'traveling_mode'] = df['traveling_mode'].replace(ts_values_dict["mode_list_cz"], ts_values_dict["mode_list"])

    #drop unidentified travelers
    df = df.dropna(subset=['traveler_id'])
    df.loc[:, 'traveler_id'] = df['traveler_id'].astype(int)

    #drop travelers with missing mandatory columns
    removed_travelers = df.loc[ (df['departure_h'].isna()) | (df['departure_m'].isna()) |
                                (df['arrival_h'].isna()) | (df['arrival_m'].isna()) |
                                (df['origin_purpose'].isna()) | (df['destination_purpose'].isna()) |
                                (df['traveling_mode'].isna())]['traveler_id'].unique()
    df = df.loc[~df['traveler_id'].isin(removed_travelers)]

    #remove travelers who do not have any destination at home
    travelers_with_home = df.loc[ (df['origin_purpose'] == 'home') | (df['destination_purpose'] == 'home') ]['traveler_id'].unique()
    df = df.loc[df['traveler_id'].isin(travelers_with_home)]

    #filter and clean trips outside the area code
    df = filter_trips_by_area(context, df)
    
    #clean and connect activity chains, purge nonsense
    df = clean_activity_chain(df)

    #convert departure and arrival time columns to seconds
    df = calculate_time_in_seconds(df)
    df = df[['traveler_id', 'trip_order', 'origin_purpose', 'destination_purpose', 'departure_time', 'arrival_time',
                'traveling_mode', 'last_trip', 'beeline', 'origin_code', 'destination_code']]
    return df

"""
Reconnect all three tables after cleaning them separately.
"""

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
    new_traveler_id = np.arange(1,len(old_traveler_id)+1)
    mapping_traveler_id = dict(zip(old_traveler_id, new_traveler_id))
    
    df_travelers.loc[:, 'traveler_id'] = df_travelers['traveler_id'].replace(mapping_traveler_id)
    df_trips.loc[:, 'traveler_id'] = df_trips['traveler_id'].replace(mapping_traveler_id)
    
    old_household_id = df_hh['household_id'].unique()
    new_household_id = np.arange(1,len(old_household_id)+1)
    mapping_household_id = dict(zip(old_household_id, new_household_id))
    
    df_hh.loc[:, 'household_id'] = df_hh['household_id'].replace(mapping_household_id)
    df_travelers.loc[:, 'household_id'] = df_travelers['household_id'].replace(mapping_household_id)

    #drop travelers in df_trips that are not in df_travelers
    df_trips = df_trips[df_trips.traveler_id.isin(df_travelers.traveler_id.unique())]

    df_hh = df_hh.reset_index(drop=True)
    df_travelers = df_travelers.reset_index(drop=True)
    df_trips = df_trips.reset_index(drop=True)

    return df_hh, df_travelers, df_trips

def return_trip_duration(arrival, departure):
    if(arrival < departure):
        midnight = 24*60*60
        return abs(arrival + (midnight - departure))
        
    return abs(arrival- departure)

def fill_traveling_mode(df_trips):
    print("Trips:", len(df_trips))
    df_trips.loc[:,"duration"] = df_trips.apply(lambda row: return_trip_duration(row.arrival_time, row.departure_time)/60, axis=1)
    df_trips.loc[:,"speed"] = (df_trips.beeline / (df_trips.duration/60))
    df_trips.loc[:,"speed"] = df_trips.speed.fillna(df_trips.speed.mean())
    df_trips.loc[:,"traveling_mode"] = df_trips.traveling_mode.replace("other", np.nan)
    print("Unique speeds",len(df_trips.speed.unique()))

    df_trips = df_trips.groupby(["speed"]).apply(lambda x: x.fillna(x.mode().iloc[0])).sort_values('traveler_id').reset_index(drop=True)
    print(df_trips[df_trips.traveling_mode.isna()])
    df_trips.loc[:,"traveling_mode"] = df_trips.traveling_mode.fillna("pt")
    print(df_trips[df_trips.traveling_mode.isna()])
    df_trips.drop(["duration","speed"], axis=1, inplace=True)
    return df_trips

def configure(context):
    context.config("data_path")
    context.config("travel_survey_files")
    context.config("prague_area_code")
    context.stage("preprocess.coded_values")
    context.stage("preprocess.clean_commute_prob")
    context.config("output_path")

def execute(context):
    #read CSV data
    encoding = "cp1250"
    df_hh = pd.read_csv(context.config("data_path") + context.config("travel_survey_files") + "H.csv", encoding=encoding, delimiter=";")
    df_persons = pd.read_csv(context.config("data_path") + context.config("travel_survey_files") + "P.csv", encoding=encoding, delimiter=";")
    df_trips = pd.read_csv(context.config("data_path") + context.config("travel_survey_files") + "T.csv", encoding=encoding, delimiter=";")

    #filter and rename columns
    df_hh = df_hh[['H_ID', 'NAZ_MOaMC', 'H_persons', 'H_venr_car_private', 'H_venr_car_company', 'H_venr_util', 'H_venr_other','H_venr_bike']]
    df_hh.columns = ['household_id', 'district_name', 'persons_number', 'car_private', 'car_company', 'car_util', 'car_other', 'bike_number']


    df_travelers = df_persons[['P_ID', 'H_ID', 'P_gender', 'P_age', 
                            'P_work', 
                            'P_driving_licence_none', 'P_triptoday', 
                            'P_caravail', 'P_bikeavail', 'P_ptavail' ]]

    df_travelers.columns = ['traveler_id', 'household_id', 'sex', 'age', 
                            'employment',
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

    df_hh = clean_household_data(context, df_hh)
    df_travelers = clean_traveler_data(context, df_travelers)
    df_trips = clean_trip_data(context, df_trips)
    
    #drop columns not useful anymore
    #df_trips = df_trips.drop(['origin_code', 'destination_code'], axis=1)
    
    #re-connect all data
    df_hh, df_travelers, df_trips = connect_tables(df_hh, df_travelers, df_trips)

    #traveling mode: replace other
    #print(df_trips.info())

    df_trips = fill_traveling_mode(df_trips)
    #print(df_trips.info())

    return df_hh, df_travelers, df_trips