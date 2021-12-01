import io, gzip
import itertools
import numpy as np
import matsim.writers as writers
from matsim.writers import backlog_iterator

PERSON_COLS = ['person_id', 'trip_today', 'localised']
ACTIVITY_COLS = ['person_id', 'activity_order', 'purpose', 'start_time', 'end_time', 'geometry', 'location_id']
TRIP_COLS = ['person_id', 'trip_order', 'traveling_mode']

def add_person(writer, person, activities, trips):
    writer.start_person(person[PERSON_COLS.index("person_id")])
    writer.start_plan(selected = True)

    for activity, trip in itertools.zip_longest(activities, trips):
         
        start_time = activity[ACTIVITY_COLS.index("start_time")]
        end_time = activity[ACTIVITY_COLS.index("end_time")]
        location_id = activity[ACTIVITY_COLS.index("location_id")]
        geometry = activity[ACTIVITY_COLS.index("geometry")]

        location = writer.location(
            -geometry.y, -geometry.x,
            None if location_id == -1 else location_id
        )

        writer.add_activity(
            type = activity[ACTIVITY_COLS.index("purpose")],
            location = location,
            start_time = None if np.isnan(start_time) else start_time,
            end_time = None if np.isnan(end_time) else end_time
        )

        if not trip is None:
            writer.add_leg(
                mode = trip[TRIP_COLS.index("traveling_mode")]
            )

    writer.end_plan()
    writer.end_person()

def configure(context):
    #type = {'home', 'work', 'education', 'shop', 'leisure', 'other' }
    #traveling_mode = {'car', 'bike', 'pt', 'walk'}
    context.stage("tests.matsim_persons_export_example")
    context.config("output_path")

def execute(context):
    output_path = context.config("output_path") + "population.xml.gz"
    df_persons, df_activities, df_trips = context.stage("tests.matsim_persons_export_example")

    df_persons = df_persons.sort_values(by=['person_id'])
    df_activities = df_activities.sort_values(by=['person_id', 'activity_order'])
    df_trips = df_trips.sort_values(by=['person_id', 'trip_order'])

    with gzip.open(output_path, 'wb+') as writer:
        with io.BufferedWriter(writer, buffer_size = 2 * 1024**3) as writer:
            writer = writers.PopulationWriter(writer)
            writer.start_population()


            activity_iterator = backlog_iterator(iter(df_activities[ACTIVITY_COLS].itertuples(index = False)))
            trip_iterator = backlog_iterator(iter(df_trips[TRIP_COLS].itertuples(index = False)))

            with context.progress(total = len(df_persons), label = "Writing population ...") as progress:
                for person in df_persons.itertuples(index = False):
                    person_id = person[PERSON_COLS.index("person_id")]

                    activities = []
                    trips = []

                    # Track all activities for person
                    while activity_iterator.has_next():
                        activity = activity_iterator.next()
                        print(person_id)
                        if not activity[ACTIVITY_COLS.index("person_id")] == person_id:
                            activity_iterator.previous()
                            break
                        else:
                            activities.append(activity)

                    assert len(activities) > 0

                    # Track all trips for person
                    while trip_iterator.has_next():
                        trip = trip_iterator.next()

                        if not trip[TRIP_COLS.index("person_id")] == person_id:
                            trip_iterator.previous()
                            break
                        else:
                            trips.append(trip)

                    assert len(trips) == len(activities) - 1

                    add_person(writer, person, activities, trips)
                    progress.update()

            writer.end_population()
    
    return "population.xml.gz"