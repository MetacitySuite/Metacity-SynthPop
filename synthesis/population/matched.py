import numpy as np

def configure(context):
    #context.stage("synthesis.population.sampled")
    #context.stage("synthesis.locations.census_home")
    context.stage("preprocess.clean_travel_survey")

def execute(context):
    #df_census = context.stage("synthesis.population.sampled")
    #df_census_home = context.stage("synthesis.locations.census_home")
    #number_of_census_persons = df_census_home["person_id"].unique().size
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")
    
    print(df_households.groupby(['district']).size())



    print(df_households['district'].unique())

    #TODO
    return 