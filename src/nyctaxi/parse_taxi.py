from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *

## Module Constants
APP_NAME = "Most Happening place"

def main(sc):
    	path = "taxi2"
    	sqlContext = SQLContext(sc)
    	taxiFile = sc.textFile(path)
	header = taxiFile.first()
	fields = [StructField(field_name, StringType(), True) for field_name in header.split(',')]

	# Fix the data types	
	fields[1].dataType = TimestampType()
	fields[2].dataType = TimestampType()
	fields[3].dataType = IntegerType()
	fields[4].dataType = FloatType()
	fields[5].dataType = FloatType()
	fields[6].dataType = FloatType()
	fields[9].dataType = FloatType()
	fields[10].dataType = FloatType()
	fields[12].dataType = FloatType()
	fields[13].dataType = FloatType()
	fields[14].dataType = FloatType()
	fields[15].dataType = FloatType()
	fields[16].dataType = FloatType()
	fields[17].dataType = FloatType()
	"""
	fields = [StructField('vendor_id',StringType(),True),
	StructField('pickup_datetime',TimestampType(),True), 
	StructField('dropoff_datetime',TimestampType(),True), 
	StructField('passenger_count',IntegerType(),True), 
	StructField('trip_distance',FloatType(),True), 
	StructField('pickup_longitude',FloatType(),True), 
	StructField('pickup_latitude',FloatType(),True), 
	StructField('rate_code',StringType(),True), 
	StructField('store_and_fwd_flag',StringType(),True), 
	StructField('dropoff_longitude',FloatType(),True), 
	StructField('dropoff_latitude',FloatType(),True), 
	StructField('payment_type',StringType(),True), 
	StructField('fare_amount',FloatType(),True), 
	StructField('surcharge',FloatType(),True), 
	StructField('mta_tax',FloatType(),True), 
	StructField('tip_amount',FloatType(),True), 
	StructField('tolls_amount',FloatType(),True), 
	StructField('total_amount',FloatType(),True)]
	"""	
	#construct schema
	schema = StructType(fields)

	# get the header and remove it from the processing
	taxiHeader = taxiFile.filter(lambda l: "vendor_id" in l)
	taxiNoHeader = taxiFile.subtract(taxiHeader)

	# split the data 
	taxi_temp = taxiNoHeader.map(lambda k: k.split(",")).map(lambda p: (p[0], datetime.strptime(p[1], "%Y-%m-%d %H:%M:%S"), datetime.strptime(p[2], "%Y-%m-%d %H:%M:%S"), int(p[3]), float(p[4]), float(p[5]) , float(p[6]), p[7] , p[8], float(p[9]), float(p[10]), p[11], float(p[12]), float(p[13]), float(p[14]), float(p[15]), float(p[16]), float(p[17]) ))
	
	
	# merge schema with the data
	#taxi_df = sqlContext.createDataFrame(taxi_temp, schema)
	taxi_temp.count()	
	#print taxi_df.count()
	#print taxiFile.count()
if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
    	sc   = SparkContext(conf=conf)

    	# Execute Main functionality
    	main(sc)