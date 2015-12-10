import json
from shapely.geometry import shape, Point

# load GeoJSON file containing sectors
with open('nyczip.geojson', 'r') as f:
    js = json.load(f)

    # construct point based on lat/long returned by geocoder
    taxi = open("/scratch/ns3184/taxi3/aa")
    #taxi = open("/scratch/ns3184/taxi3/aa")

    taxizip = open("/scratch/ns3184/taxizip/taxizipaa.csv", "w")
    i = 0
    for line in taxi.readlines():
        i+=1
	line = line[:-2]
	if i == 1:
		taxizip.write(line + ",zipcode" + "\n")
		continue
        arr = line.split(",")
	if arr[9] == "":
	       arr[9] = 0
	if arr[10] == "":
	       arr[10] = 0
	point = Point(float(arr[9]),float(arr[10]))

	# check each polygon to see if it contains the point
    	for feature in js['features']:
        	polygon = shape(feature['geometry'])
        	if polygon.contains(point):
            		taxizip.write(line + "," + feature['properties']['postalCode'] + "\n")
    taxizip.close()
