CREATE EXTERNAL TABLE IF NOT EXISTS Taxi(
	vendor_id STRING,
	pickup_datetime TIMESTAMP,
	dropoff_datetime TIMESTAMP,
	passenger_count INT,
	trip_distance INT,
	pickup_longitude DOUBLE,
	pickup_latitude DOUBLE,
	rate_code STRING,
	store_and_fwd_flag STRING,
	dropoff_longitude DOUBLE,
	dropoff_latitude DOUBLE,
	payment_type STRING,
	fare_amount DOUBLE,
	surcharge DOUBLE,
	mta_tax DOUBLE,
	tip_amount DOUBLE,
	tolls_amount DOUBLE,
	total_amount DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile location '/user/ajr619/hive_taxi'
tblproperties ("skip.header.line.count"="1");

describe Taxi;

