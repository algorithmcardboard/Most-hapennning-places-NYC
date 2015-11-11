import dotenv
import requests
import os
import sys

dotenv.load_dotenv('.env')

EF_API_KEY = os.environ.get("EF_API_KEY")
SEARCH_ENDPOINT = "http://api.eventful.com/json/events/search"

if not EF_API_KEY:
	print "no key"
	sys.exit(1)

payload = {'app_key': EF_API_KEY, 'location':'NYC', 'page_size':20}

res = requests.get(SEARCH_ENDPOINT, params=payload)

print res.text
