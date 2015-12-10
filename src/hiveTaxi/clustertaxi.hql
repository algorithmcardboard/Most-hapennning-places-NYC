drop table pickup;
CREATE EXTERNAL TABLE IF NOT EXISTS pickup (
pickup_datetime TIMESTAMP,
hour INT,
zipcode STRING);


INSERT OVERWRITE TABLE pickup select taxi.pickup_datetime,hour(pickup_datetime), zipcode.zip 
from Taxi as taxi INNER JOIN zipcode as zipcode on taxi.pickup_latitude=zipcode.latitude
and taxi.pickup_longitude=zipcode.longitude;

drop table dropoff;
CREATE EXTERNAL TABLE IF NOT EXISTS dropoff (
dropoff_datetime TIMESTAMP,
hour INT,
zipcode STRING);


INSERT OVERWRITE TABLE dropoff select taxi.dropoff_datetime, hour(dropoff_datetime), zipcode.zip
from Taxi as taxi INNER JOIN zipcode as zipcode on taxi.dropoff_latitude=zipcode.latitude
and taxi.dropoff_longitude=zipcode.longitude;

-- select * from pickup limit 10;
-- select * from dropoff limit 10;

drop table cluster;
CREATE EXTERNAL TABLE IF NOT EXISTS cluster (
date TIMESTAMP,
hour INT,
zipcode STRING,
c INT);


INSERT OVERWRITE TABLE cluster select TO_DATE(pickup_datetime) as date, hour, zipcode, count(*) as c from pickup group by TO_DATE(pickup_datetime),hour,zipcode order by hour,zipcode;




