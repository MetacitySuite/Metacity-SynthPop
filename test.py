import pandas as pd
import geopandas as gpd
import json
import numpy as np
import functools, itertools
import random
import re

def configure(context):
    context.config("data_path")
    context.config("district_mapping_file") #data sensitive
    context.stage("synthesis.locations.census_home")
    context.stage("preprocess.clean_travel_survey")
    context.stage("preprocess.zones")

def execute(context):
    df_census = context.stage("preprocess.clean_census")
    df_zones = context.stage("preprocess.zones")
    df_households, _, _ = context.stage("preprocess.clean_travel_survey")


   