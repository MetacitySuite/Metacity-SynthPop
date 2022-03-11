import pandas as pd
import numpy as np
import re
import random
from tqdm import tqdm

import synthesis.algo.other.misc as misc

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

# Defined average speeds for traveling mode - imputing purposes
max_walk_speed = 4.8
min_walk_speed = 1.5

max_car_speed = 45.
min_car_speed = 15.

max_pt_speed = 45.
min_pt_speed = 10.

max_ride_speed = 45.
min_ride_speed = 15.

min_bike_speed = 3.5
max_bike_speed = 14.
mode_speeds = [["walk", min_walk_speed, max_walk_speed],["car", min_car_speed, max_car_speed],['pt', min_pt_speed, max_pt_speed],['ride', min_ride_speed, max_ride_speed],['bike', min_bike_speed, max_bike_speed]]
    



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
    _, ts_values_dict = context.stage("data.other.coded_values")
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
        trip_count = int(row['trips'])
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

        if(trip_count > 0 and traveler_id not in (deleted_travelers)):
            try:
                first_trip = df_daily_plan.loc[(df_daily_plan['trip_order'] == 1)]
                last_trip = df_daily_plan.loc[(df_daily_plan['trip_order'] == (trip_count))]
                if(first_trip.origin_purpose.values[0] != "home" or last_trip.destination_purpose.values[0]!= "home"):
                    deleted_travelers.append(traveler_id)
            except:
                print(df_daily_plan)


    df = df.loc[~df['traveler_id'].isin(deleted_travelers)]
    return df

def delete_incomplete_chains(df):
    #check if travelers have meaningful connected trip order
    deleted_travelers = []
    primary_activities = ["home", 'work', "education"]
    variable_activities = ['shop', 'leisure', 'other']
    purposes = primary_activities + variable_activities

    day_trips = df.groupby("traveler_id")
   
    for i, day in tqdm(day_trips):
        day.sort_values(["trip_order"], inplace=True)
        traveler = day["traveler_id"].values[0]

        destinations = []
        activities = []
        last_activity = np.nan
        day_started = False
        
        for ix, trip in day.iterrows():
            if((trip.origin_purpose in purposes) and (trip.origin_purpose != last_activity)):
                day_started = True
                activities.append(trip.origin_purpose)
                last_activity = trip.origin_purpose
            if(day_started and trip.destination_purpose in purposes and ((len(activities) == 0) or (trip.destination_purpose !=activities[-1]))):
                destinations.append(trip.destination_purpose)
            
        
        if(len(destinations)>0 and len(set(activities)) > 1):
            if(destinations[-1] != activities[-1] and destinations[-1] == "home"):
                activities.append(activities[0])


            if (activities[-1] != activities[0] or activities[0] != "home"): #activity chain does not loop
                deleted_travelers.append(traveler)
                #print("Incomplete activity chain:", traveler)
                #print(day[['trip_order','origin_purpose','destination_purpose']])

    print("Deleted travelers (incomplete chains):", len(deleted_travelers))
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
    df_commute_work, df_commute_edu = context.stage("data.other.clean_commute_prob")
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

def get_trip_duration(row):
    departure = row.departure_h*3600
    departure += row.departure_m*60
    
    arrival = row.arrival_h*3600
    arrival += row.arrival_m*60
    
    duration = arrival - departure
    
    if arrival < departure:
        duration = (24*3600 - departure) + arrival
        
    return duration
        
def get_trip_speed(row):
    duration_s = row.duration
    duration_h = duration_s/3600
    
    beeline = row.beeline
    
    if(beeline != np.nan and row.beeline_valid == True):
        if(duration_h > 0):
            speed = beeline/duration_h
        else:
            speed = np.nan
        
    else: speed = np.nan
    return speed

def shift_beeline(row, min_speed, max_speed, mode):
    if(row.traveling_mode != mode):
        return row.beeline, row.speed
    
    if(row.speed > max_speed):
        new_beeline = (row.duration/3600)
        new_beeline *= max_speed
        new_speed = max_speed
        return new_beeline, new_speed
    
    if(row.speed < min_speed):
        new_beeline = (row.duration/3600)
        new_beeline *= min_speed
        new_speed = min_speed
        return new_beeline, new_speed
    
    return row.beeline, row.speed

def impute_beeline(row, mode, avg_speed, speed_std):
    if(row.traveling_mode != mode):
        return row.beeline, row.speed
    
    if(row.beeline == np.nan or row.beeline_valid == False):
        #impute beeline based on valid avg speed
        duration = row.duration/3600
        new_speed = avg_speed+random.uniform(-speed_std/2, speed_std/2)
        new_beeline = duration*new_speed
        
        return new_beeline, new_speed
        
    return row.beeline, row.speed

def delete_mode_outliers(df):
    #replace unused travel modes
    df.loc[:,'traveling_mode'] = df.traveling_mode.replace("other","pt")
    df.loc[:,'traveling_mode'] = df.traveling_mode.fillna("pt") 
    print("Trips before outlier det.:", df.shape[0])  
   
    mode_walk = df[df.traveling_mode == 'walk']
    mode_ride = df[df.traveling_mode == 'ride']
    mode_bike = df[df.traveling_mode == 'bike']
    mode_car = df[df.traveling_mode == 'car']
    mode_pt = df[df.traveling_mode == 'pt']
    print(pd.concat([mode_walk,mode_bike,mode_car, mode_ride, mode_pt]).shape[0])

    #Delete outliers
    mode_walk.drop(mode_walk[mode_walk['duration_m'] > 600].index, inplace=True)
    pt_drop_beeline = mode_pt[mode_pt['beeline'] > 100]
    pt_drop_duration= mode_pt[mode_pt['duration_m'] > 300]
    mode_pt.drop(pt_drop_duration.index, inplace=True)
    mode_pt.drop(pt_drop_beeline.index, inplace=True)
    mode_bike.drop(mode_bike[mode_bike['beeline'] > 15].index, inplace=True)
    mode_car.drop(mode_car[mode_car['beeline'] > 150].index, inplace=True)

    df = pd.concat([mode_walk,mode_bike,mode_car, mode_ride, mode_pt])
    return df


def clean_trip_data(context, df):
    #remap values
    _, ts_values_dict = context.stage("data.other.coded_values")
    df.loc[:, 'origin_purpose'] = df['origin_purpose'].replace(ts_values_dict["purpose_list_cz"], ts_values_dict["purpose_list"])
    df.loc[:, 'destination_purpose'] = df['destination_purpose'].replace(ts_values_dict["purpose_list_cz"], ts_values_dict["purpose_list"])
    df.loc[:, 'last_trip'] = df['last_trip'].replace(['ano', 'ne'], [True, False])
    df.loc[:, 'beeline_valid'] = df['beeline_valid'].replace(['ano', 'ne'], [True, False])
    df.loc[:, 'traveling_mode'] = df['traveling_mode'].replace(ts_values_dict["mode_list_cz"], ts_values_dict["mode_list"])


    #drop unidentified travelers
    df = df.dropna(subset=['traveler_id'])
    df.loc[:, 'traveler_id'] = df['traveler_id'].astype(int)
    print(df.shape)
    #drop travelers with missing mandatory columns
    removed_travelers = df.loc[ (df['departure_h'].isna()) | (df['departure_m'].isna()) |
                                (df['arrival_h'].isna()) | (df['arrival_m'].isna()) |
                                (df['origin_purpose'].isna()) | (df['destination_purpose'].isna()) |
                                (df['traveling_mode'].isna())]['traveler_id'].unique()
    df = df.loc[~df['traveler_id'].isin(removed_travelers)]
    print(df.shape)

    #remove travelers who do not have any destination at home
    travelers_with_home = df.loc[ (df['origin_purpose'] == 'home') | (df['destination_purpose'] == 'home') ]['traveler_id'].unique()
    df = df.loc[df['traveler_id'].isin(travelers_with_home)]

    #filter and clean trips outside the area code
    df = filter_trips_by_area(context, df)

    #convert departure and arrival time columns to seconds
    df = calculate_time_in_seconds(df)

    #impute duration
    df['duration'] = df.apply(lambda row: misc.return_trip_duration(row.departure_time, row.arrival_time), axis=1)
    df['duration_m'] = df.duration/60.

    #impute speed
    df['speed'] = df.apply(lambda row: get_trip_speed(row), axis=1)

    print("Trips before outlier det.:", df.shape[0])
    df = delete_mode_outliers(df)
    print("Trips after outlier det.:", df.shape[0])
    

    for mode, min_speed, max_speed in mode_speeds: 
        res = df.apply(lambda row: shift_beeline(row,min_speed,max_speed,mode), axis=1)
        df['beeline'] = [a for a,_ in res]
        df['speed'] = [b for _,b in res]

    avg_speeds = []
    df['speed'] = df.apply(lambda row: get_trip_speed(row), axis=1)

    for mode in df.traveling_mode.unique():
        avg_speed = df[df.traveling_mode == mode][df.beeline_valid == True].speed.mean()
        std_speed = df[df.traveling_mode == mode][df.beeline_valid == True].speed.std()
        print("Average speed - ",mode,"(+/-):", avg_speed, std_speed)
        avg_speeds.append((mode, avg_speed, std_speed))

    for mode, avg_speed, std_speed in avg_speeds: 
        res = df.apply(lambda row: impute_beeline(row, mode, avg_speed, std_speed), axis=1)
        df['beeline'] = [a for a,_ in res]
        df['speed'] = [b for _,b in res]
    

    #clean and connect activity chains, purge nonsense
    df = clean_activity_chain(df)
    df = delete_incomplete_chains(df)

    #print("Trips after cleaning:")
    #print(df.info())
    #for mode in df.traveling_mode.unique():
    #    print(mode)
    #    print(df[df.traveling_mode == mode].describe())


    #convert beeline to meters
    df["beeline"] = df.beeline * 1000
    df = df[['traveler_id', 'trip_order', 'origin_purpose', 'destination_purpose', 'departure_time', 'arrival_time',
                'traveling_mode', 'last_trip', 'beeline', 'origin_code', 'destination_code', 'duration']]
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


def configure(context):
    context.config("data_path")
    context.config("travel_survey_files")
    context.config("prague_area_code")
    context.config("output_path")

    context.stage("data.other.coded_values")
    context.stage("data.other.clean_commute_prob")
    

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
                        'T_dist_dir','T_mainmode', 'T_O_orp_code', 'T_D_orp_code', 'T_dist_valid']]
    
    df_trips.columns = ['traveler_id', 'trip_order', 
                        'departure_h', 'departure_m', 'arrival_h', 'arrival_m',
                        'origin_purpose', 'destination_purpose', 'last_trip',
                        'beeline', 'traveling_mode', 'origin_code', 'destination_code', 'beeline_valid']

    df_hh = clean_household_data(context, df_hh)
    df_travelers = clean_traveler_data(context, df_travelers)
    df_trips = clean_trip_data(context, df_trips)
    
    #drop columns not useful anymore
    #df_trips = df_trips.drop(['origin_code', 'destination_code'], axis=1)
    
    #re-connect all data
    df_hh, df_travelers, df_trips = connect_tables(df_hh, df_travelers, df_trips)
    #df_trips.traveling_mode.replace("ride","car_passenger", inplace=True)
    print(df_trips.info())
    print(df_travelers.info())

    return df_hh, df_travelers, df_trips