import json
from shapely.geometry import shape, Point

# load GeoJSON file containing sectors
with open('nyczip.geojson', 'r') as f:
    js = json.load(f)

    # construct point based on lat/long returned by geocoder
    point = Point(-73.994770000000003,40.736828000000003)

    # check each polygon to see if it contains the point
    for feature in js['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print 'Found containing polygon:', feature['properties']['postalCode']
