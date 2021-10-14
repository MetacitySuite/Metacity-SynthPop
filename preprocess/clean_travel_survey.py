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
    return df

def clean_traveler_data(df):
    #drop rows with incomplete data
    df = df.dropna(subset = ['sex', 'employment', 'work_status', 'car_avail'])

    employed_list_cz = ['zaměstnanec, zaměstnavatel, samostatně činný či pomáhající', 'pracující důchodce']
    student_list_cz = ['žák ZŠ', 'pracující SŠ student nebo učeň','student SŠ', 'student VŠ', 'pracující VŠ student']

    #add missing values
    df['age'] = df['age'].fillna(df.groupby(['sex', 'employment'])['age'].transform('median')).astype(int)

    #remap values
    df['sex'] = df['sex'].replace(['muž', 'žena'], ['M', 'F'])
    df['trip_today'] = df['trip_today'].replace(['ano', 'ne'], [True, False])
    df['car_avail'] = df['car_avail'].replace(['ano', 'ne'], [True, False])
    df['bike_avail'] = df['bike_avail'].replace(['ano', 'ne'], [True, False])
    df['pt_avail'] = df['pt_avail'].replace(['ano', 'ne'], [True, False])
    df['work_status'] = df['work_status'].replace(['ano', 'ne'], ['yes', 'no'])
    
    #add student as new work status
    df.loc[df['employment'].isin(student_list_cz), 'work_status'] = "student"

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


def clean_trip_data(df):
    purpose_list_cz = ['bydliště', 'práce', 'vzdělávání', 'nakupování', 'volno', 'ostatní', 'zařizování', 'prac. cesta', 'stravování']
    purpose_list = ['home', 'work', 'education', 'shop', 'leisure', 'other', 'other', 'other', 'shop']
    df['origin_purpose'] = df['origin_purpose'].replace(purpose_list_cz, purpose_list)
    df['destination_purpose'] = df['destination_purpose'].replace(purpose_list_cz, purpose_list)

    df = df.dropna(subset=['traveler_id'])
    df['traveler_id'] = df['traveler_id'].astype(int)
    #df[['origin_code', 'destination_code']].astype(int)
    #df['origin_code'] = df['origin_code'].fillna(df.groupby('traveler_id')['origin_code'].transform('median'))
    a = df.groupby('traveler_id')['origin_code'].median()
    print(a)
    #print(df[df['traveler_id'] == 1210])

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

    df_trips = df_trips[['T_ID', 'P_ID', 'T_ord', 
                        'T_O_time_hh', 'T_O_time_min', 'T_D_time_hh', 'T_D_time_min',
                        'T_O_purpose', 'T_D_purpose', 'T_last_trip',
                        'T_dist_dir','T_mainmode', 'T_O_orp_code', 'T_D_orp_code']]
    
    df_trips.columns = ['trip_id', 'traveler_id', 'trip_order', 
                        'departure_time_h', 'departure_time_m', 'arrival_time_h', 'arrival_time_m',
                        'origin_purpose', 'destination_purpose', 'last_trip',
                        'beeline', 'traveling_mode', 'origin_code', 'destination_code']

    #df_hh = clean_household_data(df_hh)
    #df_travelers = clean_traveler_data(df_travelers)
    df_trips = clean_trip_data(df_trips)
    #print(df_travelers[ (df_travelers['driving_license'] == 'ano') & (df_travelers['car_avail'] == 'ne') ] )

    #print(df_persons[df_persons['driving_license'] == 'ne']['car_avail'])





    #df_ts.columns = ["traveler_id", "household_id", "household_income", "trip_number", 
    #                    "departure_time_h", "departure_time_m", "arrival_time_h", "arrival_time_m", "trip_duration", 
    #                    "destination_purpose", "origin_purpose", "travel_distance", "mode",
    #                    "sex", "age_class", "education", "employment", "economic_activity", 
    #                    "car_avail", "bike_avail", "pt_avail"]

    #df_ts = df_ts.drop(['household_id', 'household_income', 'economic_activity', 'education'], axis=1)

    #remap values
    #df_ts['employment'] = df_ts['employment'].replace( ['žák ZŠ', 'zaměstnanec, zaměstnavatel, samostatně činný či pomáhající', 'nepracující důchodce', 
    #                                'pracující SŠ student nebo učeň', 'pracující důchodce', 'žena na mateřské dovolené',
    #                                'osoba s vlastním zdrojem obživy, na rodičovské dovolené', 'student SŠ',
    #                                'student VŠ', 'ostatní nezaměstnaní', 'nezaměstnaný hledající první zaměstnání',
    #                                'osoba v domácnosti, dítě předškolního věku, ostatní závislé osoby', 'pracující VŠ student'],
    #                            ['student', 'employed', 'unemployed', 
    #                                'student', 'employed', 'unemployed',
    #                                'unemployed', 'student', 
    #                                'student', 'unemployed', 'unemployed',
    #                                'unemployed', 'student'])
    
    #df_ts['employment'] = df_ts['employment'].replace( 
    #                    ['žák ZŠ', 'pracující SŠ student nebo učeň', 'student SŠ','student VŠ', 'pracující VŠ student'], 'student')
    #df_ts['employment'] = df_ts['employment'].replace( ['zaměstnanec, zaměstnavatel, samostatně činný či pomáhající', 
    #                                'pracující důchodce'], 'employed')
    #df_ts['employment'] = df_ts['employment'].replace( ['nepracující důchodce', 'žena na mateřské dovolené',
    #                                'osoba s vlastním zdrojem obživy, na rodičovské dovolené', 'ostatní nezaměstnaní', 
    #                                'nezaměstnaný hledající první zaměstnání',
    #                                'osoba v domácnosti, dítě předškolního věku, ostatní závislé osoby'], 'unemployed')                               
 
    #remap traveler ids
    #old_traveler_id = df_ts['traveler_id'].unique()
    #new_traveler_id = np.arange(len(df_ts['traveler_id'].unique()))
    #mapping = dict(zip(old_traveler_id, new_traveler_id))
    #df_ts['traveler_id'] = df_ts['traveler_id'].replace(mapping)
#
    #df_travelers = df_ts[['traveler_id', 'sex', 'age_class', 'employment', 'car_avail', 'bike_avail', 'pt_avail']]
    #df_travelers = df_travelers.drop_duplicates(subset ="traveler_id", keep = 'first')
    #
    #df_travelers[:, 'age_class'] = age_class_to_interval(df_travelers[:, 'age_class'])
    #print(df_travelers)
    #print(df_travelers.groupby(['sex', 'employment'], dropna=True))

    #print(df_travelers['age'].isna().sum())
    #df_trips = df_ts
    
    #return df_travelers, df_trips