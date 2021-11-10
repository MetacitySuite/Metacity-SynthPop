import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
#import seaborn as sns
#import matplotlib.pyplot as plt


"""
This stage assigns residence id for each person in the synthetic population based on the zone where the person lives.

"""

def configure(context):
    context.config("seed")
    context.stage("preprocess.clean_census")
    context.stage("preprocess.home")


"""
Sample number of houses for the people living in the zone
"""

def assign_houses(df_people, df_houses, seed):
    houses = df_houses.sample(n = len(df_people), weights='resident_number', replace = True, random_state = seed)
    df_people['residence_id'] = houses['residence_id'].values
    assigned_houses = df_people[['person_id', 'residence_id']].values
    return assigned_houses


def execute(context):
    seed = context.config("seed")
    df_census = context.stage("preprocess.clean_census")
    df_home = context.stage("preprocess.home")

    # for each zone sample enough houses for the people living in it
    all_zones = df_census['zone_id'].unique()

    #all_zones = [554782370, 554782895]
    #al = df_census.groupby(['zone_id']).size().reset_index(name='counts')
    #print(al.sort_values(by=['counts']))
    
    assigned_houses = []
    pbar = tqdm(all_zones)
    for zone in pbar:
        pbar.set_description("Assigning houses in each zone")
        df_people = df_census.loc[df_census['zone_id'] == zone]
        df_houses = df_home.loc[df_home['zone_id'] == zone]
        assigned_houses.append(assign_houses(df_people, df_houses, seed))
    
    #create a df to merge with the census
    census_with_home_coord = np.concatenate(assigned_houses, axis = 0)
    df_census_home_coord = pd.DataFrame(census_with_home_coord, columns=['person_id', 'residence_id'])
    df_census_home_coord['person_id'] = df_census_home_coord['person_id'].astype(int)
    df_census_home = df_census.merge(df_census_home_coord, on='person_id')
    
    #reset indices and reorder columns
    df_census_home = df_census_home.reset_index(drop=True)
    df_census_home = df_census_home[['person_id', 'sex', 'age', 'employment', 'residence_id', 'zone_id']]
    print(df_census_home.dtypes)
    return df_census_home