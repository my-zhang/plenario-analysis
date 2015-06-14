#!/usr/bin/env python 

import sys
import csv
import geohash
from shapely.geometry import Polygon
from shapely.geometry import Point

rdr = csv.reader(sys.stdin)

poly = Polygon([(37.795542, -122.423058), 
                (37.800019, -122.398853), 
                (37.789302, -122.38821), 
                (37.7737, -122.39542), 
                (37.770036, -122.417736)])

for r in rdr: 
    idx, gh, dt, desc = r
    if poly.contains(Point(*geohash.decode(gh))):
        print '\t'.join([gh, dt, desc])

