from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row

## Module Constants
APP_NAME = "Most Happening place"

def main(sc):
    	path = "1"
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
	df1 = df.map(lambda p: Row(id=p.id, title=p.title, lat=float(p.latitude), long=float(p.longitude), postal_code=p.postal_code, start_time=p.start_time, stop_time= p.stop_time))
 	eventsTable = sqlContext.inferSchema(df1)
	eventsTable.registerTempTable("events")
	eventsTable.printSchema()
	eventsTable.select(eventsTable.id).show()


if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
    	sc   = SparkContext(conf=conf)

    	# Execute Main functionality
    	main(sc)
