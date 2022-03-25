#TODO
import numpy as np
import pandas as pd
import tqdm


def configure(context):
    context.config("prague_area_code")
    context.stage("data.hts.clean_travel_survey")
    context.stage("synthesis.population.matched")


def execute(context):
    df_matched = context.stage("synthesis.population.matched")
    _, _, df_trips = context.stage("data.hts.clean_travel_survey")

    student = df_matched[df_matched.employment == "student"]
    #extract student people who travel to work in Prague area
    #traveler ids with work trips in Prague
    work_trips = df_trips[df_trips.destination_purpose == "education"].copy()
    prague_area = context.config("prague_area_code")

    #work trips outside prague
    trips_outside = work_trips[work_trips.destination_code != prague_area].index.to_list() + work_trips[work_trips.origin_code != prague_area].index.to_list()

    prague_trips = work_trips.drop(trips_outside)
    hts_students_prg = prague_trips.traveler_id.unique()
    hts_students_out = df_trips.iloc[trips_outside].traveler_id.unique()
    hts_students_prg = list(set(hts_students_prg) - (set(hts_students_out)))

    print("Students in Prague HTS trips:",len(hts_students_prg))
    print("Students outside with HTS trips:",len(hts_students_out))

    #print(df_matched.head())
    student = df_matched.loc[df_matched.employment == "student"]
    print("Students in census:", len(student))
    #print("student (unique HTS ids) in census:", len(student.hdm_source_id.unique()))

    student_no_trip = student[~student.hdm_source_id.isin(work_trips.traveler_id.unique())]

    student_some_trip = student[student.hdm_source_id.isin(work_trips.traveler_id.unique())]
    print("student with no valid HTS trip:", len(student_no_trip))

    student_out_trip = student_some_trip[student_some_trip.hdm_source_id.isin(hts_students_out)]
    print("student with outside HTS trip:", len(student_out_trip))

    student_trip = student_some_trip[student_some_trip.hdm_source_id.isin(hts_students_prg)]
    print("student (traveling) people in zones:",student_trip.shape[0])


    unstudent = df_matched.loc[df_matched.employment != "student"]
    unstudent_with_trip_prg = unstudent[unstudent.hdm_source_id.isin(hts_students_prg)]
    unstudent_with_trip_out = unstudent[unstudent.hdm_source_id.isin(hts_students_out)]
    print("Non-student with valid HTS trip:", len(unstudent_with_trip_prg))
    print("Non-student with outside HTS trip:", len(unstudent_with_trip_out))
    print("Non-student valid trip split\n", unstudent_with_trip_prg.employment.value_counts())

    assert(student.shape[0] == (student_trip.shape[0]+student_no_trip.shape[0]+student_out_trip.shape[0]))

    #assign commute points for student primary_trip a student_no_trip
    df_assign_school = pd.concat([student_trip, student_no_trip, unstudent_with_trip_prg])
    df_leave_home = pd.concat([student_out_trip, unstudent_with_trip_out])

    ids = set(df_assign_school.person_id.to_list() + df_leave_home.person_id.to_list())

    df_other = df_matched[~df_matched.person_id.isin(ids)]

    assert df_matched.shape[0] == (df_assign_school.shape[0]+df_other.shape[0]+df_leave_home.shape[0])
    print("People to assign school to:", df_assign_school.shape[0])

    return df_assign_school, df_other, df_leave_home




    




