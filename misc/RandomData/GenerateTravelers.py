#!/usr/bin/python
import csv
import random

records=100
print("Making %d records\n" % records)

fieldnames=['traveler_id', 'sample_weight', 'age', 'sex', 'employment', 'binary_car_availability', 'has_pt_subscription', 'home_x', 'home_y', 'primary_x', 'primary_y']
writer = csv.DictWriter(open("travelers.csv", "w"), delimiter=';', fieldnames=fieldnames)


coord_x_min = -748298
coord_x_max = -733414
coord_y_min = -1051399
coord_y_max = -1043837
sex=['m', 'f']
yn=['yes', 'no']
employment=['yes', 'no', 'student']


writer.writerow(dict(zip(fieldnames, fieldnames)))
for i in range(0, records):
  writer.writerow(dict([
    ('traveler_id', i+1),
    ('sample_weight', random.uniform(1.0, 20.0)),
    ('age', random.randint(18,60)),
    ('sex', random.choice(sex)),
    ('employment', random.choice(employment)),
    ('binary_car_availability', random.choice(yn)),
    ('has_pt_subscription', random.choice(yn)),
    ('home_x', random.randint(coord_x_min, coord_x_max)),
    ('home_y', random.randint(coord_y_min, coord_y_max)),
    ]))