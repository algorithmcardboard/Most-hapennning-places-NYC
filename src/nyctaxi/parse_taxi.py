from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *

## Module Constants
APP_NAME = "Most Happening place"

def main(sc):
    sqlContext = SQLContext(sc)
    taxiFile = sc.textFile("taxi3/aa")
    header = taxiFile.first()

    taxiHeader = taxiFile.filter(lambda l: "vendor_id" in l)
    taxiNoHeader = taxiFile.subtract(taxiHeader)

    taxi_temp = taxiNoHeader.map(lambda k: k.split(","))

    taxi_rdd = taxi_temp.map(lambda p: Row(vendor_id=p[0],
    pickup_datetime=datetime.strptime(p[1], "%Y-%m-%d %H:%M:%S"),
    dropoff_datetime=datetime.strptime(p[2], "%Y-%m-%d %H:%M:%S"),
    passenger_count=int(p[3] if p[3]!="" else 0),
    trip_distance=float(p[4] if p[4]!="" else 0),
    pickup_longitude=float(p[5] if p[5]!="" else 0) ,
    pickup_latitude=float(p[6] if p[6]!="" else 0),
    rate_code=p[7],
    store_and_fwd_flag=p[8],
    dropoff_longitude=float(p[9] if p[9]!="" else 0),
    dropoff_latitude=float(p[10] if p[10]!="" else 0),
    payment_type=p[11],
    fare_amount=float(p[12] if p[12]!="" else 0),
    surcharge=float(p[13] if p[13]!="" else 0),
    mta_tax=float(p[14] if p[14]!="" else 0),
    tip_amount=float(p[15] if p[15]!="" else 0),
    tolls_amount=float(p[16] if p[16]!="" else 0),
    total_amount=float(p[17] if p[17]!="" else 0)))

    taxi_df = sqlContext.createDataFrame(taxi_rdd)

    taxi_df.registerTempTable("taxi")

    sqlContext.registerFunction("to_hour", lambda x: x.hour)
    sqlContext.registerFunction("to_date", lambda x: x.date())
    sqlContext.registerFunction("str_date", lambda x: str(x.month) + "-" + str(x.day))
 
    th = sqlContext.sql("SELECT to_hour(dropoff_datetime) as hour, to_date(dropoff_datetime) as trip_date, dropoff_longitude as lng,dropoff_latitude as lat FROM taxi where dropoff_longitude!=0 and dropoff_latitude!=0")

    th.registerTempTable("taxi_hr")

    test_hr = sqlContext.sql("select hour, count(*) from taxi_hr group by hour,trip_date")
    #test_hr = sqlContext.sql("select hour, zipcode,trip_date, count(*) as c from taxi_hr group by hour,zipcode,trip_date order by c desc")
    print test_hr.count()

if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
