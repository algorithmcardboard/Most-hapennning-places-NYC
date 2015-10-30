#http://geopy.readthedocs.org/en/latest/

import collections
from geopy.geocoders import Nominatim

inputfile = file("../../data/taxi_data.csv")
geolocator = Nominatim()

for line in inputfile:
	latitude = line.split(',')[5]
	longitude = line.split(',')[6]
	
	print (latitude, longitude)
	location = geolocator.reverse(latitude[1:-1]+", "+longitude[1:-1])
	print (location.raw)

#from geopy.geocoders import Nominatim
#geolocator = Nominatim()
#location = geolocator.reverse("52.509669, 13.376294")
#print(location.address)
#print((location.latitude, location.longitude))
#print(location.raw)
