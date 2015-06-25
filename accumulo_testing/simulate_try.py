from itertools import chain
from simulator import plenario2expand_geog, plenario2narrow_geog, construct_transmat
from simulator import now_latlong_1, now_latlong_2, now_latlong_3, now_latlong_4, now_latlong_5
import random

date_frequency = "Placeholder"
dataset_name = "Placeholder"
now_leq_date = "01/01/2013"
now_geq_date = "12/15/2014"

now_latlong_1 = "37.795542, -122.423058"
now_latlong_2 = "37.800019, -122.398853"
now_latlong_3 = "37.789302, -122.38821"
now_latlong_4 = "37.78121, -122.39212"
now_latlong_5 = "37.770036, -122.417736"

transition_prob = construct_transmat()

print transition_prob

def format(res): 
    points = list(chain(*res))
    return zip(points[::2], points[1::2])


def simulate_try():
    if random.random() < 0.75:
        results = format(plenario2expand_geog(dataset_name, date_frequency))
    else:
        results = format(plenario2narrow_geog(dataset_name, date_frequency))
    return results
# plenario_simulator("user_login",500,dataset_name,date_freq)
