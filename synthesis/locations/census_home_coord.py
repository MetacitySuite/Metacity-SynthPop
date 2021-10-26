import pandas as pd
import geopandas as gpd
import seaborn as sns
#import matplotlib.pyplot as plt

def configure(context):
    context.config("seed")
    context.stage("preprocess.clean_census")
    context.stage("preprocess.home")
    

def execute(context):
    seed = context.config("seed")
    df_census = context.stage("preprocess.clean_census")
    df_home = context.stage("preprocess.home")

    all_zones = df_home['zone_id'].unique()

    print(all_zones)
    
    
    #home_z = df_home.loc[df_home['zone_id'] == "554782300"]
    #census_z = df_census.loc[df_census['zone_id'] == "554782300"]
    
    #houses = home_z.sample(n = len(census_z), weights='resident_number', replace = True, random_state = seed)

    #houses['count'] = houses['id'].value_counts()

    #new_houses = houses[['id', 'count']].merge(home_z, on='id', how="right")
    #new_houses['count'] = new_houses["count"].fillna(0)

    #sns.barplot(x=new_houses.id, y=new_houses['count'], color='red', alpha=0.7)
    #sns.barplot(x=new_houses.id, y=new_houses['resident_number'], color='blue', alpha=0.3)
    #plt.show()
    
    return df_home