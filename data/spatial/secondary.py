import numpy as np
import pandas as pd


def configure(context):
    context.stage("data.spatial.extract_amenities")

def execute(context):
    _, _, df_shops, df_leisure, df_other = context.stage("data.spatial.extract_amenities")
    
    df_locations = pd.DataFrame(columns=df_shops.columns)
    # Attach attributes for activity types
    df_locations = df_locations.append(df_leisure)
    df_locations["offers_leisure"] = True
    df_locations["offers_shop"] = False
    df_locations["offers_other"] = False
    df_shops["offers_shop"] = True
    df_locations = df_locations.append(df_shops)
    df_other["offers_other"] = True
    df_locations = df_locations.append(df_other)
    df_locations[["offers_leisure","offers_shop","offers_other"]] = df_locations[["offers_leisure","offers_shop","offers_other"]].replace(np.nan, False)

    # Define new IDs
    df_locations["location_id"] = np.arange(len(df_locations))
    df_locations["location_id"] = "sec_" + df_locations["location_id"].astype(str)

    #print("Secondary locations:")
    #print(df_locations.info())
    return df_locations