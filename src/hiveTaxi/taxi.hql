DROP table Taxi;
CREATE EXTERNAL TABLE IF NOT EXISTS Taxi(
	vendor_id STRING,
	pickup_datetime TIMESTAMP,
	dropoff_datetime TIMESTAMP,
	passenger_count INT,
	trip_distance INT,
	pickup_longitude DECIMAL(6,4),
	pickup_latitude DECIMAL(6,4),
	rate_code STRING,
	store_and_fwd_flag STRING,
	dropoff_longitude DECIMAL(6,4),
	dropoff_latitude DECIMAL(6,4),
	payment_type STRING,
	fare_amount DOUBLE,
	surcharge DOUBLE,
	mta_tax DOUBLE,
	tip_amount DOUBLE,
	tolls_amount DOUBLE,
	total_amount DOUBLE)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile location '/user/ns3184/hivetaxi'
tblproperties ("skip.header.line.count"="1");

ALTER TABLE Taxi ADD COLUMNS (pickup_zipcode DOUBLE, dropoff_zipcode DOUBLE);

describe Taxi;

