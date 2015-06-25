import pandas as pd
import numpy as np
import geohash
import datetime as dt
import random
import time
from shapely.geometry import Polygon
from shapely.geometry import Point


# Import Accumulo
from pyaccumulo import Accumulo, Mutation, Range

select_data = pd.read_csv("/home/ubuntu/select_data.csv")

# Connecting to Accumulo
conn = Accumulo(host="172.31.3.218",port=42424,user="root",password="plenario")

table = "Plenario_data"
conn.create_table(table)
# Writing Mutation
wr = conn.create_batch_writer(table)

for num in range(select_data.shape[0]):
    if (num%100000==0):
        print num
    m = Mutation(str(select_data.get_value(num,"Geohash")))
    # A mutation is an object that represents a row in the Accumulo Table
    m.put(cf=str(select_data.get_value(num,"Formated_date")), val=select_data.get_value(num,"Descript"))
#     m.put(cf="cf2", val="%d"%num)
    # Adding the row to the table    

    wr.add_mutation(m)

wr.close()

