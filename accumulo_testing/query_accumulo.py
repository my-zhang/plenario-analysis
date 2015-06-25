import pandas as pd
import numpy as np
import geohash
import datetime as dt
import random
import time
from shapely.geometry import Polygon
from shapely.geometry import Point
from simulate_try import simulate_try


def geohash_min_max(polyon): 
    x1, y1, x2, y2 = polyon.bounds
    return (min(geohash.encode(x1, y2), geohash.encode(x1, y2), 
                geohash.encode(x2, y1), geohash.encode(x2, y2)), 
            max(geohash.encode(x1, y2), geohash.encode(x1, y2), 
                geohash.encode(x2, y1), geohash.encode(x2, y2)))

# Import Accumulo
from pyaccumulo import Accumulo, Mutation, Range

# Connecting to Accumulo
conn = Accumulo(host="172.31.3.218",port=42424,user="root",password="plenario")
table = "Plenario_data"

# poly = Polygon([(37.795542, -122.423058), 
#                (37.800019, -122.398853), 
#                (37.789302, -122.38821), 
#                (37.7737, -122.39542), 
#                (37.770036, -122.417736)])
now_latlong_1 = "37.795542, -122.423058"
now_latlong_2 = "37.800019, -122.398853"
now_latlong_3 = "37.789302, -122.38821"
now_latlong_4 = "37.78121, -122.39212"
now_latlong_5 = "37.770036, -122.417736"

for i in range(0,20):
    poly = Polygon(simulate_try())
    
    # scan the entire table with 10 threads
    count = 0
    min_gh, max_gh = geohash_min_max(poly)

    for entry in conn.batch_scan(table, numthreads=10, 
            scanranges=[Range(srow=min_gh, erow=max_gh)]):
        count += 1
        if count%10000 == 0:
            print count
        if poly.contains(Point(*geohash.decode(entry.row))):
            print entry.row, entry.cf, entry.ts, entry.val
    print poly.bounds
    print poly.area
    print "Done With One Query %d!!!"%(i)
