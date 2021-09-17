#!/usr/bin/python
import csv
import random
import datetime

def get_travel_modes(traveler):
    available_travel_mode = ['walk']

    if(traveler['binary_car_availability']):
        available_travel_mode.append('car')
    if(traveler['has_pt_subscription']):
        available_travel_mode.append('pt')

    chosen_mode = random.choice(available_travel_mode)
    if(chosen_mode == 'car'):
        available_travel_mode.remove('pt')
        available_travel_mode.remove('walk')
    if(chosen_mode == 'pt' or chosen_mode == 'walk'):
        available_travel_mode.remove('car')
    return available_travel_mode

def generate_trip(traveler, possible_travel_mode, o_purpose, d_purpose, d_time, primary = True):
    secondary_purpose = ['shop', 'leisure', 'other']
    chosen_mode = random.choice(possible_travel_mode)
    
    if(chosen_mode == 'walk'):
        duration = random.randint(5,20)
        travel_time = datetime.timedelta(minutes=duration)
    else:
        duration = random.randint(5,60)
        travel_time = datetime.timedelta(minutes=duration)
    
    offset = random.randint(-60,60);
    departure_time = d_time + datetime.timedelta(minutes=offset)
    arrival_time = departure_time + travel_time

    activity_duration = 0
    if (primary):
        #from home
        if(o_purpose == 'home'): 
            if(traveler['employment'] == 'yes'):
                d_purpose = 'work'
            elif(traveler['employment'] == 'student'):
                d_purpose = 'education'
            else:
                d_purpose = random.choice(secondary_purpose)
        else: #return home
            d_purpose = 'home'
        
        activity_duration = random.randint(360, 480)
    else:
        d_purpose = random.choice(secondary_purpose)
        activity_duration = random.randint(20, 120)

    a_duration = datetime.timedelta(minutes=activity_duration)
    return chosen_mode, o_purpose, d_purpose, departure_time, arrival_time, a_duration

def main():
    fieldnames_traveleres=['traveler_id', 'sample_weight', 'age', 'sex', 'employment', 'binary_car_availability', 'has_pt_subscription', 'home_x', 'home_y', 'primary_x', 'primary_y']
    travelers_file = csv.DictReader(open("travelers.csv"), delimiter=';', fieldnames=fieldnames_traveleres)
    next(travelers_file)

    fieldnames_trips=['traveler_id', 'trip_number', 'traveling_mode', 'origin_purpose', 'destination_purpose', 'departure_time', 'arrival_time']
    writer = csv.DictWriter(open("trips.csv", "w"), delimiter=';', fieldnames=fieldnames_trips)
    writer.writerow(dict(zip(fieldnames_trips, fieldnames_trips)))

    departure_time_home = datetime.datetime(100, 1, 1, 8, 0, 0)

    for t in travelers_file:
        possible_travel_mode = get_travel_modes(t)
        trips = []

        """
        #primary trip
        mode = ''

        d_time = datetime.datetime(100, 1, 1, 8, 0, 0)
        a_time = datetime.datetime(100, 1, 1, 8, 0, 0)
        activity_duration = datetime.timedelta(minutes=0)
        """

        trip_no = 1
        mode, o_purpose, d_purpose, d_time, a_time, activity_duration = generate_trip(t, possible_travel_mode, 'home', '', departure_time_home, True)

        trips.append(dict([
            ('traveler_id', t['traveler_id']),
            ('trip_number', trip_no),
            ('traveling_mode', mode),
            ('origin_purpose', o_purpose),
            ('destination_purpose', d_purpose),
            ('departure_time', d_time.strftime('%H:%M')),
            ('arrival_time', a_time.strftime('%H:%M')),
        ]))

        #traveler will have a secondary trip with a probability
        if (random.random() < 0.5):
            trip_no += 1
            mode, o_purpose, d_purpose, d_time, a_time, activity_duration = generate_trip(t, possible_travel_mode, d_purpose, '', a_time + activity_duration, False)     
            trips.append(dict([
                ('traveler_id', t['traveler_id']),
                ('trip_number', trip_no),
                ('traveling_mode', mode),
                ('origin_purpose', o_purpose),
                ('destination_purpose', d_purpose),
                ('departure_time', d_time.strftime('%H:%M')),
                ('arrival_time', a_time.strftime('%H:%M')),
            ]))

            trip_no += 1
            mode, o_purpose, d_purpose, d_time, a_time, activity_duration = generate_trip(t, possible_travel_mode, d_purpose, 'home', a_time+activity_duration, True)
        
            trips.append(dict([
                ('traveler_id', t['traveler_id']),
                ('trip_number', trip_no),
                ('traveling_mode', mode),
                ('origin_purpose', o_purpose),
                ('destination_purpose', d_purpose),
                ('departure_time', d_time.strftime('%H:%M')),
                ('arrival_time', a_time.strftime('%H:%M')),
            ]))

        else:
            trip_no += 1
            mode, o_purpose, d_purpose, d_time, a_time, activity_duration = generate_trip(t, possible_travel_mode, d_purpose, 'home', a_time + activity_duration, True)
        
            trips.append(dict([
                ('traveler_id', t['traveler_id']),
                ('trip_number', trip_no),
                ('traveling_mode', mode),
                ('origin_purpose', o_purpose),
                ('destination_purpose', d_purpose),
                ('departure_time', d_time.strftime('%H:%M')),
                ('arrival_time', a_time.strftime('%H:%M')),
            ]))
        
        for trip in trips:
            writer.writerow(trip)
            #print(trip)


if __name__ == "__main__":
    main()