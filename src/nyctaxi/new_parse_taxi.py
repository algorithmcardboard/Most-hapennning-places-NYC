from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *

## Module Constants
APP_NAME = "Most Happening place"

def main(sc):
    sqlContext = SQLContext(sc)
    taxiFile = sc.textFile("taxizip1.csv")
    header = taxiFile.first()

    fields = [StructField(field_name, StringType(), True) for field_name in header.split(',')]
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

    schema = StructType(fields)
    taxiHeader = taxiFile.filter(lambda l: "vendor_id" in l)
    taxiNoHeader = taxiFile.subtract(taxiHeader)

    taxi_temp = taxiNoHeader.map(lambda k: k.split(","))

    taxi_rdd = taxi_temp.map(lambda p: (p[0],
    datetime.strptime(p[1], "%Y-%m-%d %H:%M:%S"),
    datetime.strptime(p[2], "%Y-%m-%d %H:%M:%S"),
    int(p[3] if p[3]!="" else 0),
    float(p[4] if p[4]!="" else 0),
    float(p[5] if p[5]!="" else 0) ,
    float(p[6] if p[6]!="" else 0),
    p[7],
    p[8],
    float(p[9] if p[9]!="" else 0),
    float(p[10] if p[10]!="" else 0),
    p[11],
    float(p[12] if p[12]!="" else 0),
    float(p[13] if p[13]!="" else 0),
    float(p[14] if p[14]!="" else 0),
    float(p[15] if p[15]!="" else 0),
    float(p[16] if p[16]!="" else 0),
    float(p[17] if p[17]!="" else 0),
    p[18] ))


    taxi_df = sqlContext.createDataFrame(taxi_rdd, schema)

    taxi_df.registerTempTable("taxi")

    sqlContext.registerFunction("to_hour", lambda x: x.hour)
    sqlContext.registerFunction("to_date", lambda x: x.date())
    sqlContext.registerFunction("str_date", lambda x: str(x.month) + "-" + str(x.day))
 
    th = sqlContext.sql("SELECT to_hour(dropoff_datetime) as hour, to_date(dropoff_datetime) as trip_date, dropoff_longitude as lng,dropoff_latitude as lat,zipcode FROM taxi where dropoff_longitude!=0 and dropoff_latitude!=0")

    th.registerTempTable("taxi_hr")

    #test_hr = sqlContext.sql("select hour, count(*) from taxi_hr group by hour,trip_date")
    test_hr = sqlContext.sql("select hour, zipcode,trip_date, count(*) as c from taxi_hr group by hour,zipcode,trip_date order by c desc")

if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
