from simulator import plenario2expand_geog, plenario2narrow_geog, input_global_variables
import time
import random


# Try simulating!!!

date_freq = "day"
dataset_name = "Plenario_test_7"
now_leq_date = "01/01/2013"
now_geq_date = "12/15/2014"


# Number of sessions
for k in range(5):

    # Use this function to set the initial polygon vertices coordinates
    input_global_variables("37.795542, -122.423058","37.800019, -122.398853","37.789302, -122.38821","37.78121, -122.39212","37.770036, -122.417736")

    # Individual queries after user starts workload
    for i in range(10):
        if random.random() > 0.5:
            result =  plenario2expand_geog(dataset_name, date_freq)
        else:
            result =  plenario2narrow_geog(dataset_name, date_freq)
        print result

    time.sleep(3)

# This is blocked out because we are only testing expand and narrow geog, instead of the full package
# transition_prob = construct_transmat()
# plenario_simulator("user_login",500,dataset_name,date_freq)

