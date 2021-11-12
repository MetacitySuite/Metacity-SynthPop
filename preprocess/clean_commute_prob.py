import pandas as pd

def configure(context):
    context.config("data_path")
    context.config("commute_probability_file")

def execute(context):
    df_commmute = pd.read_csv(context.config("data_path") + context.config("commute_probability_file"), delimiter=";")
    df_commmute = df_commmute[['uzemi_bydliste', 'uzemi_dojizdka', 'n', 'prob', 'aktivita']]
    df_commmute.columns = ['home_zone_id', 'commute_zone_id', 'person_number', 'probability', 'commute_purpose']
    df_commmute.loc[:, 'commute_purpose'] = df_commmute['commute_purpose'].replace(['prace', 'vzdelavani'], ['work', 'education'])
    df_commmute.loc[:, 'commute_zone_id'] = df_commmute['commute_zone_id'].replace(['mimo_obec'], [999])
    
    df_commmute.loc[:, 'home_zone_id'] = df_commmute['home_zone_id'].astype(int)
    df_commmute.loc[:, 'commute_zone_id'] = df_commmute['commute_zone_id'].astype(int)
    df_commmute.loc[:, 'probability'] = df_commmute['probability'].str.replace(',','.').astype(float)
    
    df_commute_work = df_commmute.loc[df_commmute['commute_purpose'] == 'work']
    df_coommute_education = df_commmute.loc[df_commmute['commute_purpose'] == 'education']
    return df_commute_work, df_coommute_education
