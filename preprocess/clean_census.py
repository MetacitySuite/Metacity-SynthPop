import pandas as pd

def configure(context):
    context.config("data_path")
    context.config("census_file")

def execute(context):
    df_census = pd.read_csv(context.config("data_path") + context.config("census_file"))

    #TODO
    df_census_cleaned = df_census
    
    return df_census_cleaned