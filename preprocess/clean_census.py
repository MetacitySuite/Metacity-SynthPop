import pandas as pd
import numpy as np

"""
Preprocesses and cleans full census data.
"""

def remap_values(context, df):
    #get values for remapping
    census_values_dict, _ = context.stage("preprocess.coded_values")

    df = df.dropna(subset = ['employment', 'sex', 'zone_id']) 
    
    #fill NA values
    df['age'].replace(999, np.nan, inplace=True)
    df.loc[:, 'age'] = df['age'].fillna(df.groupby(['sex', 'employment'])['age'].transform('median')).astype(int)
    df['employment'].replace(99, np.nan, inplace=True)
    df = df.groupby(['sex', 'age']).apply(lambda x: x.fillna(x.mode().iloc[0])).sort_values('person_id').reset_index(drop=True)
    
    #remap values
    df.loc[:, 'sex'] = df['sex'].replace([1, 2], ['M', 'F'])
    df.loc[:, 'employment'] = df['employment'].replace(census_values_dict['employment_values'], census_values_dict['employment_list'])
    return df

def filter_by_region(df, region_id):
    df = df.loc[df['region_id'] == region_id]
    df = df.drop(['region_id'], axis=1)
    return df

def configure(context):
    context.config("data_path")
    context.config("census_file")
    context.stage("preprocess.coded_values")
    context.config("output_path")

def execute(context):
    df_census = pd.read_csv(context.config("data_path") + context.config("census_file"), delimiter=";")
    df_census = df_census[['ID', 'KODKRAJE','kodUzemi', 'POHLAVI', 'VEK', 'LIDEKAKTI']]
    df_census.columns = ['person_id', 'region_id', 'zone_id', 'sex', 'age', 'employment']
    
    #chosen area only, Prague = 3018
    df_census = filter_by_region(df_census, 3018)
    #df_census.to_csv(context.config("output_path") + "sldb_praha.csv", index = False, sep=';')

    #fill in and remap missing values
    df_census = remap_values(context, df_census)
    df_census = df_census.reset_index(drop=True)
    return df_census