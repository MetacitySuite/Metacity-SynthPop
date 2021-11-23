import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
#import seaborn as sns
#import matplotlib.pyplot as plt


"""
This stage assigns facility id for each working person in the synthetic population 
based on the zone where the person lives, the zone-zone commute probability and the commute distance (TS) 
between residence id and candidate work destinations.

"""

def configure(context):
    context.config("seed")
    context.stage("preprocess.clean_census")
    context.stage("preprocess.home")
    context.stage("synthesis.locations.census_home")


"""

"""

def assign_facility(df_people, df_facilities, seed):
    pass


def execute(context):
    pass