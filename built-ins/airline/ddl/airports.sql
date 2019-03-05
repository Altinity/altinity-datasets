CREATE TABLE IF NOT EXISTS airports (
  AirportID String, 
  Name String,
  City String, 
  Country String,
  IATA String, 
  ICAO String, 
  Latitude Float32,
  Longitude Float32,
  Altitude Int32,
  Timezone Float32, 
  DST String,
  Tz String, 
  Type String, 
  Source String)
Engine=MergeTree()
PRIMARY KEY AirportID
PARTITION BY Country
ORDER BY AirportID
