import numpy as np
import pandas as pd

def configure(context):
    context.stage("data.hts.clean_travel_survey")


def return_trip_duration(start_time, end_time):
    if(start_time == np.nan or end_time == np.nan):
        return np.nan
    
    
    if(start_time > end_time):
        midnight = 24*60*60
        return abs(end_time + (midnight - start_time))

    return abs(start_time - end_time)

def calculate_bounds(values, bin_size):
    values = np.sort(values)

    bounds = []
    current_count = 0
    previous_bound = None

    for value in values:
        if value == previous_bound:
            continue

        if current_count < bin_size:
            current_count += 1
        else:
            current_count = 0
            bounds.append(value)
            previous_bound = value

    bounds[-1] = np.inf
    return bounds


def execute(context):
    # Prepare data HTS
    _, df_persons, df_trips = context.stage("data.hts.clean_travel_survey")

    df_persons["weight"] = 1./df_persons.shape[0]

    df_trips = pd.merge(df_trips, df_persons[["traveler_id", "weight"]]#.rename(columns = { "person_weight": "weight" })
)
    df_trips["travel_time"] = df_trips.apply(lambda row: return_trip_duration(row.departure_time, row.arrival_time), axis=1)
    print(df_trips.describe())
    df_trips = df_trips.sort_values(["traveler_id","trip_order"])

    df_trips.rename(columns={"origin_purpose":"preceeding_purpose", "destination_purpose": "following_purpose", "traveling_mode":"mode"}, inplace=True) 
    

    distance_column = "beeline"
    df = df_trips[["mode", "travel_time", distance_column, "weight", "preceeding_purpose", "following_purpose"]].rename(columns = { distance_column: "distance" })

    # Filtering
    primary_activities = ["home", "work", "education"]
    df = df[~(
        df["preceeding_purpose"].isin(primary_activities) &
        df["following_purpose"].isin(primary_activities)
    )]

    # Calculate distributions
    modes = df["mode"].unique()
    print(df.info())
    print(df["mode"].value_counts())
    

    bin_size = 20 #200
    distributions = {}

    for mode in modes:
        # First calculate bounds by unique values
        f_mode = df["mode"] == mode
        bounds = calculate_bounds(df[f_mode]["travel_time"].values, bin_size)
        print(bounds)

        distributions[mode] = dict(bounds = np.array(bounds), distributions = [])

        # Second, calculate distribution per band
        for lower_bound, upper_bound in zip([-np.inf] + bounds[:-1], bounds):
            f_bound = (df["travel_time"] > lower_bound) & (df["travel_time"] <= upper_bound)

            # Set up distribution
            values = df[f_mode & f_bound]["distance"].values
            weights = df[f_mode & f_bound]["weight"].values

            sorter = np.argsort(values)
            values = values[sorter]
            weights = weights[sorter]

            cdf = np.cumsum(weights)
            cdf /= cdf[-1]

            # Write distribution
            distributions[mode]["distributions"].append(dict(cdf = cdf, values = values, weights = weights))

    return distributions