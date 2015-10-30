#http://geopy.readthedocs.org/en/latest/

from geopy.geocoders import Nominatim
geolocator = Nominatim()
location = geolocator.reverse("-73.984138, 40.726317")
print(location.address)
print((location.latitude, location.longitude))
print(location.raw)
