from numpy.core.numeric import empty_like
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from shapely.geometry import Point

"""
This stage provides a list of work places that serve as potential locations for
work activities.

Zones which do not have any registered enterprise receive a fake work
place POINT at their centroid.
"""

def configure(context):
    context.config("seed")
    context.stage("data.spatial.zones")
    context.stage("data.spatial.extract_amenities")
    context.stage("synthesis.spatial.primary.extract_commute_trips")



def extract_facility_candidates(f_kk, df_workplaces, sample_seed, zones):
    C_kk = pd.DataFrame(columns=["commute_point","commute_zone_id", "home_zone_id"])
    print(df_workplaces.head())
    
    workplace_points = np.array([])
    commute_zones = np.array([])
    home_zones = np.array([])
    no_workplace = 0

    for zone, df_org in tqdm(f_kk.groupby("origin")):
        trips_cum = 0
        if(zone == 999):
            print("Home destination is outside Prague.")

        for i, df_dest in df_org.iterrows():
            dest = df_dest.destination
            n_trips = df_dest.trip_count
            trips_cum +=n_trips
            
            if(n_trips > 0):
                if(df_workplaces[df_workplaces.zone_id == dest].shape[0] > 0):
                    samples = df_workplaces[df_workplaces.zone_id == dest].sample(n=n_trips, replace=True, 
                                    weights=None, random_state=sample_seed)
                    if(len(samples) < n_trips):
                        print("Less samples than trips!")

                    workplace_points = np.append(workplace_points, samples.geometry)
                else:
                    #print("no destinations", dest)
                    if(dest != 999):
                        no_workplace += n_trips
                        centroid = zones[zones.zone_id == dest].zone_centroid
                    else:
                        no_workplace += n_trips
                        centroid = gpd.GeoSeries([Point(-1.0, -1.0)])
                    #print(centroid, type(centroid))
                    workplace_points = np.append(workplace_points,[centroid]*n_trips)
                
                commute_zones = np.append(commute_zones,[dest]*n_trips)
                if(len(commute_zones) != len(workplace_points)):
                    print(n_trips)
                    print("Commute", len(commute_zones), len(workplace_points))
                    break
        
        home_zones = np.append(home_zones,[zone]*trips_cum)

        if(len(home_zones) != len(workplace_points)):
            print("Home")
            break

            
    #print(len(workplace_points), len(commute_zones), len(home_zones))
    C_kk.commute_point = workplace_points
    C_kk.commute_zone_id = commute_zones
    C_kk.home_zone_id = home_zones
    print("Cumulative number of trips:", f_kk.trip_count.sum())
    print("Mass of trips available:", len(C_kk))
    return C_kk


def execute(context):
    df_zones = context.stage("data.spatial.zones")
    df_workplaces, df_schools, _, _, _ = context.stage("data.spatial.extract_amenities")
    sample_seed = context.config("seed")

    f_kk_work, df_employed, f_kk_edu, df_students = context.stage("synthesis.spatial.primary.extract_commute_trips")
    print("Abstract work trips:", f_kk_work.trip_count.sum())
    print("Employed travellers:",len(df_employed))
    print("Abstract school trips:", f_kk_edu.trip_count.sum())
    print("Studying travellers:",len(df_students))

    print("Missing values? (f_kk_work):", not f_kk_work[f_kk_work.isna().any(axis=1)].empty)
    print("Missing values? (f_kk_edu):", not f_kk_edu[f_kk_edu.isna().any(axis=1)].empty)
    #Assigning primary location (work): Step 2
    #sample destination candidates with replacement for each f_kk in 
    C_kk_work = extract_facility_candidates(f_kk_work, df_workplaces, sample_seed, df_zones)
    C_kk_edu = extract_facility_candidates(f_kk_edu, df_schools, sample_seed, df_zones)
    return C_kk_work, C_kk_edu

