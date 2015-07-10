
```python
import geohash
from shapely.geometry import Polygon
from shapely.geometry import Point
from pyaccumulo import Accumulo, Mutation, Range

def geohash_min_max(polyon): 
    '''get geohash range of given polygon object.'''
    x1, y1, x2, y2 = polyon.bounds
    return (min(geohash.encode(x1, y2), geohash.encode(x1, y2), 
                geohash.encode(x2, y1), geohash.encode(x2, y2)), 
            max(geohash.encode(x1, y2), geohash.encode(x1, y2), 
                geohash.encode(x2, y1), geohash.encode(x2, y2)))

# connecting to Accumulo
conn = Accumulo(host="", port=42424, user="root", password="passwd")
table = "Plenario_data"

# a sample polygon created by hand. 
poly = Polygon([(37.795542, -122.423058), 
               (37.800019, -122.398853), 
               (37.789302, -122.38821), 
               (37.7737, -122.39542), 
               (37.770036, -122.417736)])

min_gh, max_gh = geohash_min_max(poly)

for entry in conn.batch_scan(table, numthreads=10, 
        scanranges=[Range(srow=min_gh, erow=max_gh)]):
    if poly.contains(Point(*geohash.decode(entry.row))):
        print entry.row, entry.cf, entry.ts, entry.val
```

