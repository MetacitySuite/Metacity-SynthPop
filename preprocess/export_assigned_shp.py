from typing import ValuesView
from unicodedata import normalize
from numpy.random.mtrand import hypergeometric, seed
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from shapely.geometry import LineString, Point, point
from multiprocessing import Pool, cpu_count
import os
#import seaborn as sns
#import matplotlib.pyplot as plt


"""


"""

def configure(context):
    context.config("output_path")
    context.stage("synthesis.population.assigned")


"""

"""
def export_shp(df, output_shp):
    travels = gpd.GeoDataFrame(df)
    print(travels.head())
    travels.loc[:,"geometry"] = travels.geometry.apply(lambda point: Point(-point.x, -point.y))
    print(travels.head())

    travels.to_file(output_shp)
    print("Saved to:", output_shp)
    return


def export_activity(df, activity, context):
    print("Exporting SHPs")

    df_a_home = df[df.purpose == activity]
    df_a_home.drop_duplicates(["person_id"], inplace=True)
    df_a_home[df_a_home.location_id != np.nan]
    print(df_a_home.head())

    #val_c = df_a_home.groupby("geometry")
    #print(len(val_c))
    df_a = pd.DataFrame()
    df_a.loc[:,"geometry"] = df_a_home.geometry
    #df_a.loc[:,"activities"] = [ df.shape[0] for g, df in val_c]
    export_shp(df_a, context.config("output_path")+"activities_"+activity+".shp")
    return



def execute(context):
    df_persons, df_activities, df_ttrips = context.stage("synthesis.population.assigned")
    export_activity(df_activities, "home", context)
    export_activity(df_activities, "work", context)
    export_activity(df_activities, "education", context)

    
    return


