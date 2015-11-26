import json

txt = open("/home/ns3184/project/Most-hapennning-places-NYC/data/events/1")
res_json = json.loads(txt.read())


for event in res_json['events']['event']:
	print event['id']
