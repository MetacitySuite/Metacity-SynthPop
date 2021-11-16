

def configure(context):
    context.stage("synthesis.locations.census_home")
    context.stage("synthesis.population.matched")

def execute(context):
    #TODO
    df_census_home_coords = context.stage("synthesis.locations.census_home")
    df_matched = context.stage("synthesis.population.matched")

    df_matched = df_matched.merge(df_census_home_coords, on="person_id")