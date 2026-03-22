Services run from a single binary.

We'll have a web page with a map showing status of addresses and buildings (exists in OSM, exists in Gov data exists in both) and some simple interface to download data for the chosen area.

Backend will need to be single process and multithreaded since DuckDB does not allow multiple processes as writers and wrapping it in FlightSQL server or something would make it more complicated.

I imagine the binary would have multiple commands like:
- import
    - OSM - full osm import. Downloads PBF (from OSM France [extracts](https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf)) extract for Poland and imports required data (addresses, buildings) into the database.
    - PRG - full import of gov's address data. Downloads ZIP and parses it using my prg_convert library and imports it into the database.
    - BDOT10k - full import of gov's building data from BDOT10k registry. Downloads [ZIP](https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/Polska_BDOT10k_GeoParquet.zip), extracts the right file and imports it into the database.
    - EGIB - full import of gov's building data from EGIB registry. Downloads [data](https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet) and imports it into the database.
    - full - run all of the above
- update (mostly useful after import or after service was not running for longer period of time so it updates faster than by waiting for background thread of running service to catch up)
    - OSM - downloads minutely updates (from OSM FR [replication feed](https://download.openstreetmap.fr/replication/europe/poland/minute/)) and applies them to OSM data stored in the database
    - PRG, BDOT10k, EGIB - download new files for given dataset and update data for that dataset in the database
- run - runs the service and listens to HTTP requests, in background it updates data periodically (each dataset has its own update period)

Probably there should be a flag allowing to specify some config file with parameters like path to database file etc.

Incremental updates should be in small batches in background so it doesn't disrupt the service.

Running service needs to provide API endpoints for:
- download of data package - parameters determining which dataset (PRG, BDOT10k, EGIB) and what area (bbox in GET request, GeoJSON with Polygon in POST request)
- serving Vector Tiles - {z}/{x}/{y}.{type} (it is to be determined if type will be Mapbox Vector Tiles {mvt} or Maplibre Vector Tiles {mlt}), with data to show on a map (addresses, buildings)
- possibly geojson with info about latest updated areas
- old app had a way to report (endpoint with POST request containing IDs of records that should be excluded) that some records should not be ignored (e.g. error is source data, comparison doesn't work due to differences in schema, etc), maybe this functionality should be retained
- old app had endpoint for returning pseudo-random locations (latitude, longitude), intention was that user could just click a button on the page and have the map load some location that had some data fore review, maybe this functionality should be retained as well but it's not a high priority

We probably want to keep some form of aggregation when displaying data on lower zoom levels. Previous app was using DBSCAN algorithm to cluster objects together. In this version we can use similar approach or consider another one with e.g. H3 cells which may be cheaper computationally.

Either way we'd probably want to cache tiles from lower zoom levels or all zoom levels (like 5-14) and refresh them periodically in the background.
