from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from datetime import datetime

## Module Constants
APP_NAME = "Most Happening place"

def main(sc):
    	path = "events"
    	#text_file = sc.textFile(path)
    	sqlContext = SQLContext(sc)
    	events = sqlContext.jsonFile(path)
	#events.printSchema()    
    	#events.select(events["events.event"]).show()
    	#sqlContext.registerDataFrameAsTable(events, "events")
	#df  = sqlContext.sql("select events.event from events")
	#df.select("event.title").collect()
	#df1 = df.flatMap(lambda p: p.event)
	#df1.map(lambda x : x.title).filter(lambda x: 'hi' in x).collect()


	######
	df = events.select(events["events.event"]).flatMap(lambda p: p.event)
	df1 = df.map(lambda p: Row(
		id=p.id,\
		title=p.title, \
		lat=p.latitude, \
		long=p.longitude, \
		postal_code=p.postal_code, \
		start_time=datetime.strptime(p.start_time, "%Y-%m-%d %H:%M:%S"), \
		stop_time=p.stop_time)) 	
	df2 = sqlContext.createDataFrame(df1)
	eventsTable = sqlContext.inferSchema(df1)
	eventsTable.registerTempTable("events")
	eventsTable.printSchema()
	#print eventsTable.select(eventsTable.id).count()
	#print df2.groupBy('postal_code').count().show()
	print df2.where("lat is not null and long is not null").groupBy("lat", "long").count().show()
	#print df2.where("postal_code is not null").groupBy("postal_code").count().show()
	#print df2.where("postal_code is not null").count()
	#print df2.where("lat is null").count()
	#print df2.where("long is null").count()



if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
    	sc   = SparkContext(conf=conf)

    	# Execute Main functionality
    	main(sc)
