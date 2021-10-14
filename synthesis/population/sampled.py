

def configure(context):
    context.stage("preprocess.clean_census")

def execute(context):
    df_census = context.stage("preprocess.clean_census")
    
    #TODO
    df_census_sampled = df_census
    
    return 