import pandas as pd
import numpy as np
import geopandas as gpd
import shapely.geometry as geo
import pprint

from synthesis.algorithms.secondary.problems import find_assignment_problems
from synthesis.algorithms.secondary.rda import AssignmentSolver, DiscretizationErrorObjective, GravityChainSolver
from synthesis.algorithms.secondary.components import CustomDistanceSampler, CustomDiscretizationSolver

#import seaborn as sns
#import matplotlib.pyplot as plt

WALKING_DIST = 50


"""
#TODO

"""

def configure(context):
    context.config("seed")
    context.config("epsg")
    context.config("data_path")
    context.config("output_path")
    context.config("secondary_location_processes")
    
    context.stage("preprocess.secondary")
    context.stage("synthesis.population.matched")
    context.stage("synthesis.population.assigned")
    context.stage("synthesis.population.spatial.secondary.distance_distributions")
    
    

def return_trip_duration(start_time, end_time):
    if(start_time == np.nan or end_time == np.nan):
        return np.nan
    
    if(start_time > end_time):
        midnight = 24*60*60
        return abs(start_time + (midnight - end_time))

    return abs(end_time - start_time)


def prepare_locations(df_activities):
    # Load persons and their primary locations
    df_home = pd.DataFrame()
    df_home =  df_activities[df_activities.purpose == "home"]
    df_home = df_home.drop_duplicates(subset="person_id", keep="first")
    df_home.rename(columns={"geometry":"home"}, inplace=True)
    #df_home["home"][geo.Point(px,py) for px, py in list(zip(df_home["x"].values.tolist(), df_home["y"].values.tolist()))]

    #df_work["work"] = [geo.Point(px,py) for px, py in list(zip(df_work["x"].values.tolist(), df_work["y"].values.tolist()))]
    df_work = pd.DataFrame()
    df_work =  df_activities[df_activities.purpose == "work"]
    df_work = df_work.drop_duplicates(subset="person_id", keep="first")
    df_work.rename(columns={"geometry":"work"}, inplace=True)

    #df_education["education"] = [geo.Point(px,py) for px, py in list(zip(df_education["x"].values.tolist(), df_education["y"].values.tolist()))]
    df_education = pd.DataFrame()
    df_education =  df_activities[df_activities.purpose == "education"]
    df_education = df_education.drop_duplicates(subset="person_id", keep="first")
    df_education.rename(columns={"geometry":"education"}, inplace=True)

    #df_persons = context.stage("synthesis.population.sampled")[["person_id", "household_id"]]
    df_locations = df_home[["person_id","home"]].copy()
    df_locations = pd.merge(df_locations, df_work[["person_id", "work"]], how = "left", on = "person_id")
    df_locations = pd.merge(df_locations, df_education[["person_id", "education"]], how = "left", on = "person_id")

    return df_locations[["person_id", "home", "work", "education"]].sort_values(by = "person_id")


def prepare_secondary(df_destinations):
    df_destinations.rename(columns = {"location_id": "destination_id"}, inplace = True)

    identifiers = df_destinations["destination_id"].values
    locations = np.vstack(df_destinations["geometry"].apply(lambda x: np.array([-x.x, -x.y])).values) ## kladny 5514, jako df_primary ano nedava to smysl

    data = {}

    for purpose in ("shop", "leisure", "other"):
        f = df_destinations["offers_%s" % purpose].values

        data[purpose] = dict(
            identifiers = identifiers[f],
            locations = locations[f]
        )

    return data

def prepare_trips(df_trips, df_activities):
    df_activities["departure_order"] = df_activities.activity_order - 1
    df_trips['start'] = df_trips.merge(df_activities[['end_time','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").end_time.values

    df_trips['preceeding_purpose'] = df_trips.merge(df_activities[['purpose','person_id','activity_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","activity_order"], how="left").purpose.values

    df_trips['end'] = df_trips.merge(df_activities[['start_time','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").start_time.values

    df_trips['following_purpose'] = df_trips.merge(df_activities[['purpose','person_id','departure_order']], 
            left_on=["person_id","trip_order"],
            right_on=["person_id","departure_order"], how="left").purpose.values

    df_trips.loc[:,"travel_time"] = df_trips.apply(lambda row: return_trip_duration(row.start, row.end), axis=1) 

    df_trips.rename(columns={"traveling_mode":"mode"}, inplace=True)


    FIELDS = ["person_id", "trip_id", "preceeding_purpose", "following_purpose", "mode", "travel_time"] #TODO
    #trip_id
    df_trips["trip_id"] = df_trips.trip_order.values

    df_trips = df_trips[FIELDS]
    df_trips = df_trips.sort_values(["person_id", "trip_id"])
    return df_trips


def resample_cdf(cdf, factor):
    if factor >= 0.0:
        cdf = cdf * (1.0 + factor * np.arange(1, len(cdf) + 1) / len(cdf))
    else:
        cdf = cdf * (1.0 + abs(factor) - abs(factor) * np.arange(1, len(cdf) + 1) / len(cdf))

    cdf /= cdf[-1]
    return cdf

def resample_distributions(distributions, factors):
    for mode, mode_distributions in distributions.items():
        for distribution in mode_distributions["distributions"]:
            distribution["cdf"] = resample_cdf(distribution["cdf"], factors[mode])


def print_person(person_id, df_persons, df_activities, df_trips):
    print("Person:")
    print(df_persons[df_persons.person_id == person_id])
    print("Activities:")
    print(df_activities[df_activities.person_id == person_id])
    print("Trips:")
    print(df_trips[df_trips.person_id == person_id])


def remove_ids(remove_ids, df_persons, df_activities, df_trips):
    df_persons = df_persons[~df_persons.person_id.isin(remove_ids)]
    df_activities = df_activities[~df_activities.person_id.isin(remove_ids)]
    df_trips = df_trips[~df_trips.person_id.isin(remove_ids)]

    return df_persons, df_activities, df_trips
"""

"""
def execute(context):
    df_persons, df_activities, df_trips = context.stage("synthesis.population.assigned")
    df_destinations = context.stage("preprocess.secondary")

    #trips
    df_trips = prepare_trips(df_trips, df_activities)

    #print_person(144, df_persons, df_activities, df_trips) #o-h-o invalid chain, o-h-o-l-h-o
    #print_person(949584, df_persons, df_activities, df_trips) #o-h-o invalid chain

    #invalid_chains = [144,219,260,1516,1681,1916,2104,3830,949584,78464,953,1111,1190, 2384,3312,11930,25859,958365] #TODO remove invalid chains in hts export
    #print_person(157538, df_persons, df_activities, df_trips)

    print(df_activities.purpose.unique()) 
    #TODO export all activities
    print(df_trips.following_purpose.unique())
    print(df_trips.preceeding_purpose.unique())

    # primary locations and secondary destinations
    df_primary = prepare_locations(df_activities)
    destinations = prepare_secondary(df_destinations)

    # Prepare data
    distance_distributions = context.stage("synthesis.population.spatial.secondary.distance_distributions")
    # Resampling for calibration TODO
    resample_distributions(distance_distributions, dict(
        #car = 0.3, ride = 0.0, pt = 1.0, walk = -0.1, bike = -0.1
        car = 0.0, ride = 0.0, pt = 0.5, walk = 0.0, bike = 0.0
    ))

    # Segment into subsamples
    processes = context.config("secondary_location_processes")

    unique_person_ids = df_trips["person_id"].unique() #
    number_of_persons = len(unique_person_ids)
    print("Processing persons:", number_of_persons)
    number_w_secondary = len(set(df_activities[df_activities.purpose.isin(["leisure","shop","other"])].person_id.values))
    print("Persons with secondary destinations:", number_w_secondary, number_w_secondary/number_of_persons, "%")
    unique_person_ids = np.array_split(unique_person_ids, processes)

    random = np.random.RandomState(context.config("seed"))
    random_seeds = random.randint(10000, size = processes)

    # Create batch problems for parallelization
    batches = []

    for index in range(processes):
        batches.append((
            df_trips[df_trips["person_id"].isin(unique_person_ids[index])],
            df_primary[df_primary["person_id"].isin(unique_person_ids[index])],
            random_seeds[index]
        ))

    # Run algorithm in parallel
    with context.progress(label = "Assigning secondary locations to persons", total = number_of_persons):
        with context.parallel(processes = processes, data = dict(
            distance_distributions = distance_distributions,
            destinations = destinations
        )) as parallel:
            df_locations, df_convergence = [], []

            for df_locations_item, df_convergence_item in parallel.imap_unordered(process, batches):
                df_locations.append(df_locations_item)
                df_convergence.append(df_convergence_item)

    df_locations = pd.concat(df_locations).sort_values(by = ["person_id", "trip_index"])
    print("Locations:", df_locations.shape)
    df_convergence = pd.concat(df_convergence)

    print("Success rate:", df_convergence["valid"].mean())

    return df_locations, df_convergence
    

    
def process(context, arguments):
  df_trips, df_primary, random_seed = arguments

  # Set up RNG
  random = np.random.RandomState(context.config("seed"))

  # Set up distance sampler
  distance_distributions = context.data("distance_distributions")
  #print(distance_distributions)
  distance_sampler = CustomDistanceSampler(
        maximum_iterations = 100,#1000
        random = random,
        distributions = distance_distributions)

  # Set up relaxation solver; currently, we do not consider tail problems.
  gamma = 20.0
  delta_p = 0.1

  relaxation_solver = GravityChainSolver(
    random = random, eps = 20.0, lateral_deviation = 20.0, alpha = 0.3
    #lateral deviation in meters (sigma)
    # displacemenet factor delta_p 0.1 (?)
    #convergence threshold in meters
    )

    #lateral deviation 10
  # Set up discretization solver
  destinations = context.data("destinations")
  discretization_solver = CustomDiscretizationSolver(destinations)

  # Maximum discretization errors

  #thresholds = dict(
  #  car = 200.0, ride= 200.0, pt = 200.0,
  #  bike = 100.0, walk = 100.0
  #)

  thresholds = dict(
    car = 1000.0, ride= 1000.0, pt = 1000.0,
    bike = 1000.0, walk = 750.0
  )

  assignment_objective = DiscretizationErrorObjective(thresholds = thresholds)
  assignment_solver = AssignmentSolver(
      distance_sampler = distance_sampler,
      relaxation_solver = relaxation_solver,
      discretization_solver = discretization_solver,
      objective = assignment_objective,
      maximum_iterations = 20 #20
      )

  df_locations = []
  df_convergence = []

  last_person_id = None

  for problem in find_assignment_problems(df_trips, df_primary):    
      result = assignment_solver.solve(problem)
      #pprint.pprint(result)

      starting_trip_index = problem["trip_index"]

      for index, (identifier, location) in enumerate(zip(result["discretization"]["identifiers"], result["discretization"]["locations"])):
          df_locations.append((
              problem["person_id"], starting_trip_index + index, identifier, geo.Point(location)
          ))

      df_convergence.append((
          result["valid"], problem["size"]
      ))

      if problem["person_id"] != last_person_id:
          last_person_id = problem["person_id"]
          context.progress.update()

  df_locations = pd.DataFrame.from_records(df_locations, columns = ["person_id", "trip_index", "destination_id", "geometry"])
  df_locations = gpd.GeoDataFrame(df_locations, crs = "EPSG:5514")

  df_convergence = pd.DataFrame.from_records(df_convergence, columns = ["valid", "size"])
  return df_locations, df_convergence