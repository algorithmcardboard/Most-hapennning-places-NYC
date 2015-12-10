import dotenv
import requests
import os
import sys
import json
import time

dotenv.load_dotenv('.env')

EF_API_KEY = os.environ.get("EF_API_KEY")
SEARCH_ENDPOINT = "http://api.eventful.com/json/events/search"

if not EF_API_KEY:
	print "no key"
	sys.exit(1)

payload = {'app_key': EF_API_KEY, 'location':'40.758899,-73.9873197', 'within': 30, 'date': 'Past', 'page_size' : 100}
res = requests.get(SEARCH_ENDPOINT, params=payload)
res_json = json.loads(res.text.encode("utf-8"))
count = res_json['page_count']

print count

for i in range(int(count)):
	print i+1
	event_payload = {'app_key': EF_API_KEY, 'location':'40.758899,-73.9873197', 'within': 30, 'date': 'Past', 'page_number': i + 1, 'page_size' : 100}
	res = requests.get(SEARCH_ENDPOINT, params=event_payload)

	with open(os.path.join("/scratch/ns3184/taxi_latlng/", str(i+1)), "w") as file:
		file.write(res.text.encode("utf-8"))

	time.sleep(3)
#print res.text.encode("utf-8")
