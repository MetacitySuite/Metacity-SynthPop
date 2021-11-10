import numpy as np

def configure(context):
    #context.stage("synthesis.population.sampled")
    context.config("data_path")
    context.config("district_mapping_file") #data sensitive
    context.stage("preprocess.clean_census")
    context.stage("preprocess.clean_travel_survey")

def execute(context):
    #df_census = context.stage("synthesis.population.sampled")
    df_census = context.stage("preprocess.clean_census")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")

    #print(df_census)
    print(df_travelers)
    #TODO
    return 