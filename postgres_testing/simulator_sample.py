from simulator import *

# Try simulating!!!

date_freq = "day"
dataset_name = "Plenario_test_7"
now_leq_date = "01/01/2013"
now_geq_date = "12/15/2014"
now_latlong_1 = "70.00,100.00"
now_latlong_2 = "71.00,100.00"
now_latlong_3 = "71.00,102.00"
now_latlong_4 = "70.00,102.00"
now_latlong_5 = "69.00,101.00"

transition_prob = construct_transmat()
plenario_simulator("user_login",500,dataset_name,date_freq)
