import numpy as np

def configure(context):
    context.stage("synthesis.population.sampled")
    context.stage("synthesis.population.clean_travel_survey")

def execute(context):
    df_census = context.stage("synthesis.population.sampled")
    number_of_census_persons = df_census["person_id"].unique().size

    df_travelers, _ = context.stage("synthesis.population.clean_travel_survey")
    
    df_target = df_census

    #TODO
    df_matched = df_target
    
    return df_matched