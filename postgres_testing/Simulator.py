


"""
Plenario Simulator Groundwork


1. Identify states & Transition between states

2. Construct Markov Chain Transition Matrix

3. Assume query functions are built

4. Simulator: Map Transition Matrix to queries and then send them

"""



import numpy as np
import random
import time
from sql import *
import pandas as pd
import psycopg2
import sys
import datetime
import matplotlib as plt
import geohash



# 1. Identify states & Transition between states

query_states = ["add_data","user_login","query_data","expand_time","narrow_time","expand_geog","narrow_geog"]


# 2. Construct Default Markov Chain Transition Matrix

def construct_transmat():
    query_states = ["add_data","user_login","query_data","expand_time","narrow_time","expand_geog","narrow_geog"]
    transition_prob = {}
    for state in query_states:
        transition_prob[state] = {}
    
    level_1_states = ["add_data","user_login"]
    for state in level_1_states:        
        for state_2 in query_states:
            transition_prob[state][state_2] = 0
        transition_prob[state]["user_login"] = 0
        transition_prob[state]["add_data"] = 0.01
        transition_prob[state]["query_data"] = 0.99
        
        
    level_2_states = ["query_data"]
    for state in level_2_states:
        for state_2 in query_states:
            transition_prob[state][state_2] = 0
        transition_prob[state]["add_data"] = 0.01
        transition_prob[state]["expand_time"] = min((1-sum(transition_prob[state].values())),0.25)
        transition_prob[state]["narrow_time"] = min((1-sum(transition_prob[state].values())),0.25)
        transition_prob[state]["expand_geog"] = min((1-sum(transition_prob[state].values())),0.25)
        transition_prob[state]["narrow_geog"] = min((1-sum(transition_prob[state].values())),0.25)

    level_3_states = ["expand_time","narrow_time","expand_geog","narrow_geog"]
    for state in level_3_states:
        for state_2 in query_states:
            transition_prob[state][state_2] = 0
        transition_prob[state]["add_data"] = 0.01
        transition_prob[state]["expand_time"] = min((1-sum(transition_prob[state].values())),0.25)
        transition_prob[state]["narrow_time"] = min((1-sum(transition_prob[state].values())),0.25)
        transition_prob[state]["expand_geog"] = min((1-sum(transition_prob[state].values())),0.25)
        transition_prob[state]["narrow_geog"] = min((1-sum(transition_prob[state].values())),0.25)
        
    return transition_prob

# Function to alter transition_matrix
def alter_transmat(transition_prob,curr_state,next_state,p):
    transition_prob[curr_state][next_state] = 0
    if p < (1-sum(transition_prob[curr_state].values())):
        transition_prob[curr_state][next_state] = p
        return transition_prob
    else:
        print "Total probability for state (%s) exceeds 1"%(curr_state)
        transition_prob[curr_state][next_state] = p
        return transition_prob
    


# In[3]:

# 3. Assume query functions are built

def plenario2add_data(dataset_name,date_freq):
    return "This is just a PLACEHOLDER for now"
    


# In[4]:

# Simulator for mapping and sending queries

def plenario_simulator(start_state,count,dataset_name,date_freq):
    # Construct a mapper for queries
    Queries_lib = {}
    for state in query_states:
        function_name = "plenario2" + state
        Queries_lib[state] = function_name
    
    
    print "simulate start"
    
    current_state = ""
    # Initialization
    function_name = Queries_lib[start_state]
    result = globals()[function_name](dataset_name,date_freq)    
    current_state = start_state
    
    for i in range(count):
        print current_state
    # Build range for probabilities
        prob_range = {}
        current_max = 0
        for state in query_states:
            prob_range[state] = [current_max,(current_max+transition_prob[current_state][state])]
            current_max += transition_prob[current_state][state]
        # Can actually build a large base and just call from there --> saves recomputation
        
    # Generate random number and map to transition matrix
        generate_num = random.random()
        print generate_num
        k = 0
        stop = 0;
        while ((k < len(prob_range)) and (stop==0)):
            if (generate_num > prob_range[prob_range.keys()[k]][0]) and (generate_num < prob_range[prob_range.keys()[k]][1]):
                pick_state = prob_range.keys()[k]
                k += 1
                stop = 1
            else:
                k += 1
                
        function_name = Queries_lib[pick_state]
        result = globals()[function_name](dataset_name,date_freq)    
        
        # Re-update the new state
        current_state = pick_state
    
    # Need to add in variables here
    # result = globals()[function_name]()    
    
    print "simulate ends"


"""
# Using psycopg 2 to connect to postgres
# Connect to an existing database --> input database name and username
conn = psycopg2.connect("dbname=plenario_test user=ubuntu")

#  Open a cursor to perform database operations
cur = conn.cursor()

# Execute a command: Can just enter the command we need cur.execute()

# Query the database and obtain data as Python objects
cur.execute("SELECT * FROM test;")
cur.fetchone()

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()

"""

# Login to postgres
def plenario2user_login(dbname, username):
    conn = psycopg2.connect("dbname=%s user=%s")%(dbname, username)    
    cur = conn.cursor()
    print "done"
    
# plenario2query_data
def plenario2query_data(dataset_name,date_freq):
    
    leq_date = "01/01/2013"
    geq_date = "12/15/2014"
    latlong_1 = "100.00,70.00"
    latlong_2 = "101.00,71.00"
    latlong_3 = "102.00,72.00"
    latlong_4 = "103.00,71.00"
    latlong_5 = "101.50,70.00"

    global now_leq_date
    now_leq_date = leq_date
    global now_geq_date
    now_geq_date = geq_date
    global now_latlong_1
    now_latlong_1 = latlong_1
    global now_latlong_2
    now_latlong_2 = latlong_2
    global now_latlong_3
    now_latlong_3 = latlong_3
    global now_latlong_4
    now_latlong_4 = latlong_4
    global now_latlong_5
    now_latlong_5 = latlong_5

    query = "SELECT date_trunc(%s, dat_master.obs_date) AS date_trunc_1, count(dat_master.obs_date) AS count_1, dat_master.%s AS dat_master_%s FROM dat_master WHERE dat_master.current_flag = true AND ST_Within(dat_master.location_geom, ST_GeomFromGeoJSON(\'{\"crs    \": {\"type\": \"name\", \"properties\": {\"name\": \"EPSG:4326\"}}, \"type\": \"Polygon\", \"coordinates\": [[[%s], [%s], [%s], [%s], [%s]]]}\'))AND dat_master.obs_date >= %s AND dat_master.obs_date <= %s AND dat_master.%s = in_%s GROUP BY dat_master.%s, date_trunc(%s, dat_master.obs_date) ORDER BY date_trunc(%s, dat_master.obs_date);"%(date_freq, dataset_name, dataset_name, latlong_1, latlong_2, latlong_3, latlong_4, latlong_5, leq_date, geq_date, dataset_name, dataset_name, dataset_name, date_freq, date_freq)
    cur.execute(query)
    print "Done"

# plenario2expand_time
def plenario2expand_time(dataset_name,date_freq):
    global now_leq_date 
    global now_geq_date 

    leq_date = now_leq_date.split("/")[0] + "/" + now_leq_date.split("/")[1] + "/" + str(max(1900,int(now_leq_date.split("/") [2]) - 1))
    geq_date = now_geq_date.split("/")[0] + "/" + now_geq_date.split("/")[1] + "/" + str(min(2016,int(now_geq_date.split("/") [2]) + 1))
    
    query = "SELECT date_trunc(%s, dat_master.obs_date) AS date_trunc_1, count(dat_master.obs_date) AS count_1, dat_master.%s AS dat_master_%s FROM dat_master WHERE dat_master.current_flag = true AND ST_Within(dat_master.location_geom, ST_GeomFromGeoJSON(\'{\"crs    \": {\"type\": \"name\", \"properties\": {\"name\": \"EPSG:4326\"}}, \"type\": \"Polygon\", \"coordinates\": [[[%s], [%s], [%s], [%s], [%s]]]}\'))AND dat_master.obs_date >= %s AND dat_master.obs_date <= %s AND dat_master.%s = in_%s GROUP BY dat_master.%s, date_trunc(%s, dat_master.obs_date) ORDER BY date_trunc(%s, dat_master.obs_date);"%(date_freq, dataset_name, dataset_name, now_latlong_1, now_latlong_2, now_latlong_3, now_latlong_4, now_latlong_5, leq_date, geq_date, dataset_name, dataset_name, dataset_name, date_freq, date_freq)
    cur.execute(query)
    now_leq_date = leq_date
    now_geq_date = geq_date

    print "Done expand time"

# plenario2narrow_time
def plenario2narrow_time(dataset_name,date_freq):
    global now_leq_date 
    global now_geq_date 
    
    leq_date = now_leq_date.split("/")[0] + "/" + now_leq_date.split("/")[1] + "/" + str(max(1900,int(now_leq_date.split("/") [2]) + 1))
    geq_date = now_geq_date.split("/")[0] + "/" + now_geq_date.split("/")[1] + "/" + str(min(2016,int(now_geq_date.split("/") [2]) - 1))
    
    query = "SELECT date_trunc(%s, dat_master.obs_date) AS date_trunc_1, count(dat_master.obs_date) AS count_1, dat_master.%s AS dat_master_%s FROM dat_master WHERE dat_master.current_flag = true AND ST_Within(dat_master.location_geom, ST_GeomFromGeoJSON(\'{\"crs    \": {\"type\": \"name\", \"properties\": {\"name\": \"EPSG:4326\"}}, \"type\": \"Polygon\", \"coordinates\": [[[%s], [%s], [%s], [%s], [%s]]]}\'))AND dat_master.obs_date >= %s AND dat_master.obs_date <= %s AND dat_master.%s = in_%s GROUP BY dat_master.%s, date_trunc(%s, dat_master.obs_date) ORDER BY date_trunc(%s, dat_master.obs_date);"%(date_freq, dataset_name, dataset_name, now_latlong_1, now_latlong_2, now_latlong_3, now_latlong_4, now_latlong_5, leq_date, geq_date, dataset_name, dataset_name, dataset_name, date_freq, date_freq)
    cur.execute(query)
    now_leq_date= leq_date
    now_geq_date = geq_date
    
    print "Done narrow time"



def plenario2expand_geog(dataset_name,date_freq):
    global now_latlong_1
    global now_latlong_2
    global now_latlong_3
    global now_latlong_4
    global now_latlong_5

    #     Identify Box
    lat_list = []
    long_list = []
    lat_list.append(float(now_latlong_1.split(",")[0]))
    long_list.append(float(now_latlong_1.split(",")[1]))
    lat_list.append(float(now_latlong_2.split(",")[0]))
    long_list.append(float(now_latlong_2.split(",")[1]))
    lat_list.append(float(now_latlong_3.split(",")[0]))
    long_list.append(float(now_latlong_3.split(",")[1]))
    lat_list.append(float(now_latlong_4.split(",")[0]))
    long_list.append(float(now_latlong_4.split(",")[1]))
    lat_list.append(float(now_latlong_5.split(",")[0]))
    long_list.append(float(now_latlong_5.split(",")[1]))

    #     Draw boundary for Box
    lat_min = min(lat_list)
    lat_max = max(lat_list)
    long_min = min(long_list) 
    long_max = max(long_list)

    lat_bound = [lat_min, lat_max]
    long_bound = [long_min, long_max]

    #     Expand Box
    expand_lat_min = lat_min - 1.25*(lat_max - lat_min)
    expand_lat_max = lat_max + 1.25*(lat_max - lat_min)
    expand_long_min = long_min - 1.25*(long_max - long_min)
    expand_long_max = long_max + 1.25*(long_max - long_min)

    lat_expand_bound = [expand_lat_min,expand_lat_max]
    long_expand_bound = [expand_long_min,expand_long_max]

    #     Divide into 4 zones
    #     Zone1
    range_1_lat = [expand_lat_min, (expand_lat_min + expand_lat_max)/2]
    range_1_long = [(expand_long_min + expand_long_max)/2, expand_long_max]
    #     Zone2
    range_2_lat = [(expand_lat_min + expand_lat_max)/2, expand_lat_max]
    range_2_long = [(expand_long_min + expand_long_max)/2, expand_long_max]
    #     Zone3
    range_3_lat = [expand_lat_min, (expand_lat_min + expand_lat_max)/2]
    range_3_long = [expand_long_min, (expand_long_min + expand_long_max)/2]
    #     Zone4
    range_4_lat = [(expand_lat_min + expand_lat_max)/2, expand_lat_max]
    range_4_long = [expand_long_min, (expand_long_min + expand_long_max)/2]

    new_points = []

    #     Determine the zones for each point
    for latlong in [now_latlong_1, now_latlong_2, now_latlong_3, now_latlong_4, now_latlong_5]:
    #         If in left side of BOX
        if range_1_lat[0] < float(latlong.split(",")[0]) < range_1_lat[1]:
    #         If in 1
            if range_1_long[0] < float(latlong.split(",")[1]) < range_1_long[1]:
                rand_lat = random.uniform(range_1_lat[0],range_1_lat[1])
    #             If in covered range
                if rand_lat < (range_1_lat[1] + range_1_lat[0])/2:
                    rand_long = random.uniform(range_1_long[0],range_1_long[1])
                else:
                    rand_long = random.uniform((range_1_long[1]+range_1_long[0])/2,range_1_long[1])
    #           If in 3
            else:
                rand_lat = random.uniform(range_3_lat[0],range_3_lat[1])
                if rand_lat < (range_3_lat[1] + range_3_lat[0])/2:
                    rand_long = random.uniform(range_3_long[0],range_3_long[1])
                else:
                    rand_long = random.uniform(range_3_long[0],(range_3_long[1]+range_3_long[0])/2)
        else:
    #             If in 2
            if range_2_long[0] < float(latlong.split(",")[1]) < range_2_long[1]:
                rand_lat = random.uniform(range_2_lat[0],range_2_lat[1])
    #                 If in covered range
                if rand_lat > (range_2_lat[1] + range_2_lat[0])/2:
                    rand_long = random.uniform(range_2_long[0],range_2_long[1])
                else:
                    rand_long = random.uniform((range_2_long[1]+range_2_long[0])/2,range_2_long[1])
            else:
                rand_lat = random.uniform(range_4_lat[0],range_4_lat[1])
                if rand_lat > (range_4_lat[1] + range_4_lat[0])/2:
                    rand_long = random.uniform(range_4_long[0],range_4_long[1])
                else:
                    rand_long = random.uniform(range_4_long[0],(range_4_long[1]+range_4_long[0])/2)
#         print rand_lat,rand_long

        new_points.append([min(90.0,max(-90.0,rand_lat)),min(180.0,max(-180.0,rand_long))])

    new_points = np.array(new_points)
    
    latlong_1 = "%s,%s"%(new_points[0][0],new_points[0][1])
    latlong_2 = "%s,%s"%(new_points[1][0],new_points[1][1])
    latlong_3 = "%s,%s"%(new_points[2][0],new_points[2][1])
    latlong_4 = "%s,%s"%(new_points[3][0],new_points[3][1])
    latlong_5 = "%s,%s"%(new_points[4][0],new_points[4][1])


    query = "SELECT date_trunc(%s, dat_master.obs_date) AS date_trunc_1, count(dat_master.obs_date) AS count_1, dat_master.%s AS dat_master_%s FROM dat_master WHERE dat_master.current_flag = true AND ST_Within(dat_master.location_geom, ST_GeomFromGeoJSON(\'{\"crs    \": {\"type\": \"name\", \"properties\": {\"name\": \"EPSG:4326\"}}, \"type\": \"Polygon\", \"coordinates\": [[[%s], [%s], [%s], [%s], [%s]]]}\'))AND dat_master.obs_date >= %s AND dat_master.obs_date <= %s AND dat_master.%s = in_%s GROUP BY dat_master.%s, date_trunc(%s, dat_master.obs_date) ORDER BY date_trunc(%s, dat_master.obs_date);"%(date_freq, dataset_name, dataset_name, latlong_1, latlong_2, latlong_3, latlong_4, latlong_5, now_leq_date, now_geq_date, dataset_name, dataset_name, dataset_name, date_freq, date_freq)
    cur.execute(query)    

    now_latlong_1 = latlong_1
    now_latlong_2 = latlong_2
    now_latlong_3 = latlong_3
    now_latlong_4 = latlong_4
    now_latlong_5 = latlong_5

    print "Done expand geog"
    return new_points



#plenario2narrow_geog
def plenario2narrow_geog(dataset_name,date_freq):
    global now_latlong_1
    global now_latlong_2
    global now_latlong_3
    global now_latlong_4
    global now_latlong_5

#     Identify Box
    lat_list = []
    long_list = []
    lat_list.append(float(now_latlong_1.split(",")[0]))
    long_list.append(float(now_latlong_1.split(",")[1]))
    lat_list.append(float(now_latlong_2.split(",")[0]))
    long_list.append(float(now_latlong_2.split(",")[1]))
    lat_list.append(float(now_latlong_3.split(",")[0]))
    long_list.append(float(now_latlong_3.split(",")[1]))
    lat_list.append(float(now_latlong_4.split(",")[0]))
    long_list.append(float(now_latlong_4.split(",")[1]))
    lat_list.append(float(now_latlong_5.split(",")[0]))
    long_list.append(float(now_latlong_5.split(",")[1]))

    #     Draw boundary for Box
    lat_min = min(lat_list)
    lat_max = max(lat_list)
    long_min = min(long_list) 
    long_max = max(long_list)

    lat_bound = [lat_min, lat_max]
    long_bound = [long_min, long_max]

    #     Expand Box
    narrow_lat_min = lat_min + 0.25*(lat_max - lat_min)
    narrow_lat_max = lat_max - 0.25*(lat_max - lat_min)
    narrow_long_min = long_min + 0.25*(long_max - long_min)
    narrow_long_max = long_max - 0.25*(long_max - long_min)

    lat_narrow_bound = [narrow_lat_min,narrow_lat_max]
    long_narrow_bound = [narrow_long_min,narrow_long_max]

    #     Divide into 4 zones
    #     Zone1
    range_1_lat = [narrow_lat_min, (narrow_lat_min + narrow_lat_max)/2]
    range_1_long = [(narrow_long_min + narrow_long_max)/2, narrow_long_max]
    #     Zone2
    range_2_lat = [(narrow_lat_min + narrow_lat_max)/2, narrow_lat_max]
    range_2_long = [(narrow_long_min +narrow_long_max)/2, narrow_long_max]
    #     Zone3
    range_3_lat = [narrow_lat_min, (narrow_lat_min + narrow_lat_max)/2]
    range_3_long = [narrow_long_min, (narrow_long_min + narrow_long_max)/2]
    #     Zone4
    range_4_lat = [(narrow_lat_min + narrow_lat_max)/2, narrow_lat_max]
    range_4_long = [narrow_long_min, (narrow_long_min + narrow_long_max)/2]


    #     Determine the zones for each point
    #       Have 4 new points in each box, and one last one simulated to box
    new_points = []
    new_points.append([random.uniform(range_1_lat[0],range_1_lat[1]),random.uniform(range_1_long[0],range_1_long[1])])
    new_points.append([random.uniform(range_2_lat[0],range_2_lat[1]),random.uniform(range_2_long[0],range_2_long[1])])
    new_points.append([random.uniform(range_3_lat[0],range_3_lat[1]),random.uniform(range_3_long[0],range_3_long[1])])
    new_points.append([random.uniform(range_4_lat[0],range_4_lat[1]),random.uniform(range_4_long[0],range_4_long[1])])
    last_rand = random.random()
    if last_rand<0.25:
        new_points.append([random.uniform(range_1_lat[0],range_1_lat[1]),random.uniform(range_1_long[0],range_1_long[1])])
    elif 0.25<last_rand < 0.5:
        new_points.append([random.uniform(range_2_lat[0],range_2_lat[1]),random.uniform(range_2_long[0],range_2_long[1])])
    elif 0.5 <last_rand<0.75:
        new_points.append([random.uniform(range_3_lat[0],range_3_lat[1]),random.uniform(range_3_long[0],range_3_long[1])])
    else:
        new_points.append([random.uniform(range_4_lat[0],range_4_lat[1]),random.uniform(range_4_long[0],range_4_long[1])])

    new_points = np.array(new_points)
    
    latlong_1 = "%s,%s"%(new_points[0][0],new_points[0][1])
    latlong_2 = "%s,%s"%(new_points[1][0],new_points[1][1])
    latlong_3 = "%s,%s"%(new_points[2][0],new_points[2][1])
    latlong_4 = "%s,%s"%(new_points[3][0],new_points[3][1])
    latlong_5 = "%s,%s"%(new_points[4][0],new_points[4][1])


    query = "SELECT date_trunc(%s, dat_master.obs_date) AS date_trunc_1, count(dat_master.obs_date) AS count_1, dat_master.%s AS dat_master_%s FROM dat_master WHERE dat_master.current_flag = true AND ST_Within(dat_master.location_geom, ST_GeomFromGeoJSON(\'{\"crs    \": {\"type\": \"name\", \"properties\": {\"name\": \"EPSG:4326\"}}, \"type\": \"Polygon\", \"coordinates\": [[[%s], [%s], [%s], [%s], [%s]]]}\'))AND dat_master.obs_date >= %s AND dat_master.obs_date <= %s AND dat_master.%s = in_%s GROUP BY dat_master.%s, date_trunc(%s, dat_master.obs_date) ORDER BY date_trunc(%s, dat_master.obs_date);"%(date_freq, dataset_name, dataset_name, latlong_1, latlong_2, latlong_3, latlong_4, latlong_5, now_leq_date, now_geq_date, dataset_name, dataset_name, dataset_name, date_freq, date_freq)
    cur.execute(query)    

    now_latlong_1 = latlong_1
    now_latlong_2 = latlong_2
    now_latlong_3 = latlong_3
    now_latlong_4 = latlong_4
    now_latlong_5 = latlong_5

    print "Done narrow geog"
    
    return new_points





