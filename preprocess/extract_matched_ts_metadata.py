


def configure(context):
    context.config("data_path")
    context.config("epsg")
    context.config("shape_file_home")
    context.stage("preprocess.clean_travel_survey")
    context.stage("synthesis.population.matched")

def execute(context):
    pass