from locale import normalize
import pandas as pd
import numpy as np
import geopandas as gpd
import shapely.geometry as geo
import pprint



WALKING_DIST = 50


"""
Prepares final synthetic population with travel demands for export.

"""
def configure(context):
    context.config("output_path")

    context.stage("synthesis.spatial.primary.assigned")
    context.stage("synthesis.spatial.secondary.assigned")


def return_trip_duration(start_time, end_time):
    if(start_time == np.nan or end_time == np.nan):
        return np.nan
    
    if(start_time > end_time):
        midnight = 24*60*60
        return abs(start_time + (midnight - end_time))

    return abs(end_time - start_time)

def export_shp(df, output_shp):
    travels = gpd.GeoDataFrame()

    travels.loc[:,"geometry"] = df.apply(lambda row: geo.LineString([row.origin, row.destination]), axis=1)
    travels.loc[:,"person_id"] = df.person_id.values
    #travels.loc[:,"beeline"] = df.beeline.values
    travels.loc[:,"dest"] = df.following_purpose.values
    travels.loc[:,"origin"] = df.preceeding_purpose.values
    travels.loc[:,"mode"] = df.traveling_mode.values
    #travels.loc[:,"travelerid"] = df.hdm_source_id.values
    #travels.loc[:,"travels"] = df.commute_point.apply(lambda point: not point.equals(geo.Point(-1.0,-1.0)))

    travels.to_file(output_shp)
    print("Saved to:", output_shp)
    return

def prepare_trips_shp(df_trips, df_activities):
    df_activities["departure_order"] = df_activities.activity_order - 1
    df_trips['start'] = df_trips.merge(df_activities[['end_time','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").end_time.values

    df_trips['preceeding_purpose'] = df_trips.merge(df_activities[['purpose','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").purpose.values

    df_trips['origin'] = df_trips.merge(df_activities[['purpose','person_id','activity_order',"geometry"]], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").geometry.values

    df_trips['end'] = df_trips.merge(df_activities[['start_time','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").start_time.values

    df_trips['following_purpose'] = df_trips.merge(df_activities[['purpose','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").purpose.values

    df_trips['destination'] = df_trips.merge(df_activities[['purpose','person_id','departure_order', "geometry"]], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").geometry.values

    df_trips.loc[:,"travel_time"] = df_trips.apply(lambda row: return_trip_duration(row.start, row.end), axis=1) 

    #df_trips.rename(columns={"traveling_mode":"mode"}, inplace=True)

    df_trips["origin"] = df_trips.origin.apply(lambda point: geo.Point(-point.x, -point.y))
    df_trips["destination"] = df_trips.destination.apply(lambda point: geo.Point(-point.x, -point.y))


    FIELDS = ["person_id", "trip_id", "preceeding_purpose", "following_purpose", "traveling_mode", "travel_time", "origin", "destination"] #TODO
    #trip_id
    df_trips["trip_id"] = df_trips.trip_order.values

    df_trips = df_trips[FIELDS]
    df_trips = df_trips.sort_values(["person_id", "trip_id"])
    return df_trips



def execute(context):
    df_persons, df_activities, df_trips = context.stage("synthesis.spatial.primary.assigned")
    df_locations, df_convergence = context.stage("synthesis.spatial.secondary.assigned")
    df_locations["activity_order"] = df_locations.trip_index + 1
    df_locations = df_locations.rename(columns={"destination_id":"location_id"})

    df_activities= df_activities.merge(df_locations[["person_id","activity_order","location_id", "geometry"]], 
                        left_on =["person_id","activity_order"],
                        right_on = ["person_id","activity_order"], how="outer", suffixes=[None, "_sec"])
    df_activities["location_id"] = df_activities["location_id"].fillna(df_activities["location_id_sec"])
    df_activities["geometry"] = df_activities["geometry"].fillna(df_activities["geometry_sec"])

    df_activities.drop(["location_id_sec","geometry_sec"], axis=1, inplace=True)

    print(df_activities.head())
    print(df_activities.info())
    print("NaN geometries:",df_activities.geometry.isna().sum())
    print(df_convergence.head())
    print("Valid secondary location ratio:", df_convergence.valid.value_counts(normalize=True))


    #car-passenger without drivers_lic and car avail
    print(df_persons.driving_license.value_counts())
    cars = df_trips[df_trips.traveling_mode == "car"]
    #pd.set_option('display.max_rows', None)
    drivers = cars.person_id.unique() #TODO driving_lic = True if drives a car
    df_persons.loc[:, "driving_license"] = df_persons.apply(lambda row: row.driving_license or row.person_id in drivers,axis=1)
    print(df_persons.driving_license.value_counts())
    print(df_trips.traveling_mode.value_counts())
    print("People traveling today:")
    print(df_persons.trip_today.value_counts())

    #export_shp(df_population_trips[df_population_trips.following_purpose == "leisure" ], context.config("output_path")+"dest_leisure_travels.shp")
    #export_shp(df_population_trips[df_population_trips.following_purpose == "home" ], context.config("output_path")+"dest_home_travels.shp")
    #export_shp(df_population_trips[df_population_trips.following_purpose == "work" ], context.config("output_path")+"dest_work_travels.shp")
    #export_shp(df_population_trips[df_population_trips.following_purpose == "education"], context.config("output_path")+"dest_education_travels.shp")
    #export_shp(df_population_trips, context.config("output_path")+"all_travels.shp")
    return df_persons, df_activities, df_trips