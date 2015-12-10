from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from datetime import datetime

## Module Constants
APP_NAME = "Most Happening place"

def toCSV(data):
    return ','.join(str(d) for d in data)

def main(sc):
    	path = "events"
    	#text_file = sc.textFile(path)
    	sqlContext = SQLContext(sc)
    	events = sqlContext.jsonFile(path)

	events = events.select(events["events.event"]).flatMap(lambda p: p.event)
	events = events.map(lambda p: Row(
		id=p.id,\
		title=p.title, \
		lat=p.latitude, \
		long=p.longitude, \
		postal_code=p.postal_code, \
		start_time=datetime.strptime(p.start_time, "%Y-%m-%d %H:%M:%S"), \
		stop_time=p.stop_time)) 	
	events_df = sqlContext.createDataFrame(events)
	
	events_df.registerTempTable("events")

	sqlContext.registerFunction("to_hour", lambda x: x.hour)
	sqlContext.registerFunction("str_date", lambda x: str(x.month) + "-" + str(x.day) + "-" + str(x.year))

	e = sqlContext.sql("select title, str_date(start_time) as event_date,
	to_hour(start_time) as hour, postal_code from events where postal_code is not null and start_time is not null")

	events_grouped = sqlContext.sql("select event_date, hour, postal_code, 
	count(*) from events_filtered group by event_date,hour,postal_code order by postal_code,hour")

	grouped_csv = events_grouped.map(toCSV)
	grouped_csv.saveAsTextFile('events_cluster')


if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
    	sc   = SparkContext(conf=conf)

    	# Execute Main functionality
    	main(sc)
