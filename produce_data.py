import pandas as pd
import numpy as np
import geohash
import datetime as dt
import random
import time
import copy

"""
The function reproduce_data here takes in an input_sample,
 which is a csv of an existing plenario dataset, and reproduces more data of different sizes.
version_name is used here just to make the dataset names different;
rows means the number of rows you want in the new dataset
"""


def random_date(lower_bound, upper_bound):
    year = random.randint(lower_bound, upper_bound)
    month = random.randint(0,12)
    day = random.randint(0,28)
    
    return "%s/%s/%s"%(month,day,year)
    

def reproduce_data(input_sample, version_name, rows):
    # sample file has 266mb, 1.5m rows
    
    # sample_data = "sfpd_incident_all_datetime.csv"
    
    sample_data = pd.read_csv(input_sample)
    snap_data = copy.deepcopy(sample_data[["X","Y","Date","Descript"]])
    snap_data["Geohash"] = ""
    snap_data["Formated_date"] = ""
    snap_data['Dataset'] = "sfpd_incident_%s"%(version_name)
    
    
    data_list = []
    
    # This loop pertubates the coordinates and changes the date randomly
    multiple = (rows / snap_data)
    residal = rows % snap_data
    for i in range(min(len(snap_data),rows)):
        if i%10000==0:
            print i
        temp = list(snap_data.loc[i])
        data_list.append(temp)
    
        for k in range(multiple):
            temp_1 = temp
            temp_1[0] += random.random() / 100
            temp_1[1] += random.random() / 100
            temp_1[2] = random_date(2010,2015)
            data_list.append(temp_1)
    
    columns = snap_data.columns
    result_data = pd.DataFrame(data_list)
    result_data.columns = columns
    
    # This step produces geohashed output
    for i in range(result_data.shape[0]):
        ghash = geohash.encode(result_data.get_value(i,"Y"),result_data.get_value(i,"X"))
        result_data.set_value(i,"Geohash",ghash)

        fdate = result_data.get_value(i,"Date").split("/")[2] + result_data.get_value(i,"Date").split("/")[1] + result_data.get_value(i,"Date").split("/")[0]
        result_data.set_value(i,"Formated_date",fdate)

        if (i%10000 ==0):
            print i
        
    return result_data
