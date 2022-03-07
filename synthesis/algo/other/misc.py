
import numpy as np
from shapely.geometry import LineString, Point


WALKING_DIST = 50
TIME_VARIATION_M = 15
MIDNIGHT = 24*60*60

def return_trip_duration(start_time, end_time):
    if(start_time == np.nan or end_time == np.nan):
        return np.nan
    
    if(start_time > end_time):
        return abs(start_time + (MIDNIGHT - end_time))

    return abs(end_time - start_time)


def return_trip_duration_row(row, next_row):
    ixr, row = row
    ixn, next_row = next_row

    if(row.person_id != next_row.person_id):
        return 0

    if(next_row.start_time == np.nan or row.end_time == np.nan):
        return np.nan
    
    if(next_row.start_time < row.end_time):
        return abs(next_row.start_time + (MIDNIGHT - row.end_time))

    return abs(next_row.start_time - row.end_time)


def return_geometry_point(df_row):
    if(df_row.purpose == "home"):
        return Point([-df_row.residence_point.x, -df_row.residence_point.y])
    if(df_row.purpose in ["work", "education"]):
        try:
            return Point([-df_row.commute_point.x, -df_row.commute_point.y])
        except:
            return None
    return None

def return_home_id(df_row):
    if(df_row.purpose == "home"):
        return df_row.residence_id
    if(df_row.purpose in ["work", "education"]):
        return np.nan
    return np.nan

def walk_short_distance(df_row):
    #speed = df_row.distance / df_row.trip_duration
    if(df_row.distance != np.nan and df_row.distance <= WALKING_DIST):
        return "walk"

    return df_row.traveling_mode

def get_distance(origin, destination):
    if(origin == None or destination == None):
        return np.nan
    else:
        try:
            return abs(origin.distance(destination))
        except:
            print(origin, destination)


def return_time_variation(df_row, column, prev_row=None, df = None):
    ixr, df_row = df_row
    if prev_row is None:
        return np.nan
    ixn, prev_row = prev_row
    
    if(np.isnan(df_row.loc[column])):
        return np.nan

    duration = df_row.trip_duration
    last_duration = prev_row.trip_duration
    shift = TIME_VARIATION_M*60

    try:
        if(last_duration == np.nan):
            last_duration = 0

        if(column == "end_time" and duration > 0):
            offset = np.random.randint(max(-shift, -duration*0.4),min(shift, duration*0.4))
        elif(column == "start_time" and last_duration > 0):
            offset = np.random.randint(max(-shift, -last_duration*0.4),min(shift, last_duration*0.4))
        else:
            offset = 0
    except ValueError:
        print(duration, last_duration, df_row[column], column)
        print(df[df.person_id == df_row.person_id][["person_id","traveler_id","purpose","start_time","end_time","trip_duration","activity_order"]])
        return df_row[column]

    new_time = df_row[column] + offset

    if(new_time > MIDNIGHT):
        return MIDNIGHT
    
    if(new_time < 0):
        return 0

    return new_time


def print_assign_results(df_persons, df_activities, df_trips):
    print("PERSONS:", df_persons.shape[0])
    print(df_persons.info())
    print(df_persons.head())
    print(df_persons.trip_today.value_counts(normalize=True))
    assert len(df_persons.person_id.unique()) == df_persons.shape[0]
    assert not df_persons.isnull().values.any()

    print("ACTIVITIES:", df_activities.shape[0])
    print(df_activities.info())
    print(df_activities.head())
    print(df_activities.purpose.value_counts(normalize=True))
    assert len(df_activities.person_id.unique()) == len(df_persons.person_id.unique())
    print("Person id in persons that are not in activities:",set(df_persons.person_id.unique()) - set(df_activities.person_id.unique()))
    

    print("TRIPS:")
    print(df_trips.info())
    print(df_trips.head())