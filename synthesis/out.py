
def configure(context):
    context.config("output_path")

    context.stage("preprocess.clean_census")
    context.stage("preprocess.clean_travel_survey")

def execute(context):
    df_census = context.stage("preprocess.clean_census")
    df_households, df_travelers, df_trips = context.stage("preprocess.clean_travel_survey")

    df_census.to_csv(context.config("output_path") + "census2011.csv", index = False, sep=';')

    df_households.to_csv(context.config("output_path") + "H-1.csv", index = False, sep=';')
    df_travelers.to_csv(context.config("output_path") + "P-1.csv", index = False, sep=';')
    df_trips.to_csv(context.config("output_path") + "T-1.csv", index = False, sep=';')
