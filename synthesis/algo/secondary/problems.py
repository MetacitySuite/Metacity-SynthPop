import numpy as np
import pandas as pd

FIELDS = ["person_id", "trip_id", "preceeding_purpose", "following_purpose", "mode", "travel_time"] #TODO
FIXED_PURPOSES = ["home", "work", "education"]

def find_bare_assignment_problems(df):
    problem = None

    for row in df[FIELDS].itertuples(index = False):
        person_id, trip_index, preceeding_purpose, following_purpose, mode, travel_time = row

        if not problem is None and person_id != problem["person_id"]:
            # We switch person, but we're still tracking a problem. This is a tail!
            yield problem
            problem = None

        if problem is None:
            # Start a new problem
            problem = dict(
                person_id = person_id, trip_index = trip_index, purposes = [preceeding_purpose],
                modes = [], travel_times = []
            )

        problem["purposes"].append(following_purpose)
        problem["modes"].append(mode)
        problem["travel_times"].append(travel_time)

        if problem["purposes"][-1] in FIXED_PURPOSES:
            # The current chain (or initial tail) ends with a fixed
            yield problem
            problem = None

 

LOCATION_FIELDS = ["person_id", "home", "work", "education"]

def find_assignment_problems(df, df_locations):
    """
        Enriches assignment problems with:
          - Locations of the fixed activities
          - Size of the problem
          - Reduces purposes to the variable ones
    """
    print(df.info())
    location_iterator = df_locations[LOCATION_FIELDS].itertuples(index = False)
    current_location = None    

    for problem in find_bare_assignment_problems(df):
        origin_purpose = problem["purposes"][0]
        destination_purpose = problem["purposes"][-1]
        pi = problem["purposes"]

        # Reduce purposes TODO wtf
        if origin_purpose in FIXED_PURPOSES and destination_purpose in FIXED_PURPOSES:
            problem["purposes"] = problem["purposes"][1:-1]

        elif origin_purpose in FIXED_PURPOSES:
            problem["purposes"] = problem["purposes"][1:]

        elif destination_purpose in FIXED_PURPOSES:
            problem["purposes"] = problem["purposes"][:-1]

        else:
            raise RuntimeError("The presented 'problem' is neither a chain nor a tail")

        # Define size
        problem["size"] = len(problem["purposes"])

        if problem["size"] == 0:
            continue # We can skip if there are no variable activities

        # Advance location iterator until we arrive at the current problem's person
        while current_location is None or current_location[0] != problem["person_id"]:
            current_location = next(location_iterator)

        # Define origin and destination locations if they have fixed purposes
        problem["origin"] = None
        problem["destination"] = None

        if origin_purpose in FIXED_PURPOSES:
            problem["origin"] = current_location[LOCATION_FIELDS.index(origin_purpose)] # Shapely POINT
            problem["origin"] = np.array([[problem["origin"].x, problem["origin"].y]])

        if destination_purpose in FIXED_PURPOSES:
            problem["destination"] = current_location[LOCATION_FIELDS.index(destination_purpose)] # Shapely POINT
            problem["destination"] = np.array([[problem["destination"].x, problem["destination"].y]])

        yield problem


