import pandas as pd
import numpy as np
import geopandas as gpd
import shapely.geometry as geo
import synthesis.algo.other.misc as misc

from synthesis.algo.secondary.problems import find_assignment_problems
from synthesis.algo.secondary.rda import AssignmentSolver, DiscretizationErrorObjective, GravityChainSolver
from synthesis.algo.secondary.components import CustomDistanceSampler, CustomDiscretizationSolver



"""
#TODO

"""

def configure(context):
    context.config("seed")
    context.config("epsg")
    context.config("data_path")
    context.config("output_path")
    context.config("secondary_location_processes")

    context.stage("synthesis.spatial.primary.assigned")
    context.stage("synthesis.spatial.secondary.distance_distributions")
    context.stage("synthesis.spatial.secondary.locations")
    


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
    df_persons, df_activities, df_trips = context.stage("synthesis.spatial.primary.assigned")
    df_trips, df_primary, destinations = context.stage("synthesis.spatial.secondary.locations")
    distance_distributions = context.stage("synthesis.spatial.secondary.distance_distributions")
    
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