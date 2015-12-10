drop table zipcode;
CREATE EXTERNAL TABLE IF NOT EXISTS zipcode_raw (
latitude DOUBLE,
longitude DOUBLE,
zip STRING)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile location '/user/ns3184/zipcode_raw';

INSERT OVERWRITE LOCAL DIRECTORY '/scratch/ns3184/zipcode' ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' select latitude,longitude,zip from zipcode_raw where zip is not NULL and zip <> '';

CREATE EXTERNAL TABLE IF NOT EXISTS zipcode (
longitude DOUBLE,
latitude DOUBLE,
zip STRING)
ROW FORMAT delimited fields terminated by ',' STORED AS textfile location '/user/ns3184/zipcode';

select * from zipcode limit 10;

