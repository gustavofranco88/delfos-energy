create table if not exists data (
    "timestamp" timestamp not null,
    wind_speed numeric(10,2) not null,
    power numeric(10,2) not null,
    ambient_temperature numeric(10,2)
); 

INSERT INTO data (timestamp, wind_speed, power, ambient_temperature)
SELECT 
    ts,
    (random() * 25) as wind_speed,          
    (random() * 2000) as power,             
    (15 + random() * 20) as ambient_temperature    
FROM generate_series(
    now() - interval '10 days', 
    now(), 
    interval '1 minute'
) as ts;