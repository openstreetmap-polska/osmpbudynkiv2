$ bash scripts/run_release.sh 
   Compiling osmpbudynkiv2 v0.1.0 (/mnt/nvme/git/osmpbudynkiv2)
    Finished `release` profile [optimized] target(s) in 43.46s
2026-08-28T15:34:56.925530Z  INFO osmpbudynkiv2: Initializing databases db_path=./osmpbudynkiv2.duckdb rocksdb_path=./osmpbudynkiv2.rocksdb
2026-08-28T15:34:57.510273Z  INFO osmpbudynkiv2::download: Downloading url="https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf" attempt=1
Downloaded poland-latest.osm.pbf
  [00:01:02] [=======================================================================================================================================================================================================================================================================================] 2.23 GiB/2.23 GiB (36.65 MiB/s, 0s)2026-08-28T15:35:59.995872Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/poland-latest.osm.pbf
2026-08-28T15:36:00.080175Z  INFO osmpbudynkiv2::import::osm: Starting OSM import path="/tmp/poland-latest.osm.pbf"
2026-08-28T15:36:00.080188Z  INFO osmpbudynkiv2::import::osm: Pass 1: Streaming nodes, ways and relations to RocksDB
2026-08-28T15:40:04.299773Z  INFO osmpbudynkiv2::import::osm: PBF streamed to RocksDB nodes=244355075 ways=33824447 relations=286717
2026-08-28T15:40:04.300272Z  INFO osmpbudynkiv2::import::osm: Step done: stream PBF to RocksDB elapsed=4m 4s
2026-08-28T15:40:04.300276Z  INFO osmpbudynkiv2::import::osm: Pass 2: Importing address nodes
2026-08-28T15:40:10.919058Z  INFO osmpbudynkiv2::import::osm: Address nodes imported count=3272502
2026-08-28T15:40:10.919073Z  INFO osmpbudynkiv2::import::osm: Step done: import address nodes elapsed=6.6s
2026-08-28T15:40:10.919077Z  INFO osmpbudynkiv2::import::osm: Importing way buildings
2026-08-28T15:41:18.570636Z  INFO osmpbudynkiv2::import::osm: Way buildings imported count=17988085
2026-08-28T15:41:18.570649Z  INFO osmpbudynkiv2::import::osm: Importing way addresses
2026-08-28T15:41:52.730163Z  INFO osmpbudynkiv2::import::osm: Way addresses imported count=5415777
2026-08-28T15:41:52.730178Z  INFO osmpbudynkiv2::import::osm: Step done: import way buildings and addresses elapsed=1m 41s
2026-08-28T15:41:52.730182Z  INFO osmpbudynkiv2::import::osm: Importing way former buildings
2026-08-28T15:41:59.384943Z  INFO osmpbudynkiv2::import::osm: Way former buildings imported count=15387
2026-08-28T15:41:59.384958Z  INFO osmpbudynkiv2::import::osm: Step done: import way former buildings elapsed=6.6s
2026-08-28T15:41:59.384968Z  INFO osmpbudynkiv2::import::osm: Importing relation buildings
2026-08-28T15:42:05.377888Z  INFO osmpbudynkiv2::import::osm: Relation buildings imported count=4905
2026-08-28T15:42:05.377901Z  INFO osmpbudynkiv2::import::osm: Importing relation addresses
2026-08-28T15:42:11.290075Z  INFO osmpbudynkiv2::import::osm: Relation addresses imported count=3663
2026-08-28T15:42:11.290092Z  INFO osmpbudynkiv2::import::osm: Step done: import relation buildings and addresses elapsed=11.9s
2026-08-28T15:42:11.290097Z  INFO osmpbudynkiv2::import::osm: Importing relation former buildings
2026-08-28T15:42:18.147765Z  INFO osmpbudynkiv2::import::osm: Relation former buildings imported count=6
2026-08-28T15:42:18.147787Z  INFO osmpbudynkiv2::import::osm: Step done: import relation former buildings elapsed=6.8s
2026-08-28T15:42:25.646826Z  WARN osmpbudynkiv2::osm::geometry: Repaired invalid OSM geometry — fix these objects in OSM to remove the need table="osm_buildings" repaired=1 dropped_degenerate=12 examples=way/251057034
2026-08-28T15:42:25.677880Z  INFO osmpbudynkiv2::import::osm: Step done: repair invalid geometry repaired=1 dropped_degenerate=12 elapsed=7.5s
2026-08-28T15:44:18.704808Z  INFO osmpbudynkiv2::import::osm: Step done: compact reverse indexes elapsed=1m 53s
2026-08-28T15:44:18.704821Z  INFO osmpbudynkiv2::import::osm: Creating spatial indexes
2026-08-28T15:44:23.417896Z  INFO osmpbudynkiv2::import::osm: Step done: create spatial indexes elapsed=4.7s
2026-08-28T15:44:23.423841Z  INFO osmpbudynkiv2::import::osm: OSM replication metadata from PBF header sequence=7260352 timestamp="2026-08-27T01:38:49Z"
2026-08-28T15:44:23.425283Z  INFO osmpbudynkiv2::import::osm: OSM import totals buildings=17992978 addresses=8691942 former_buildings=15393
2026-08-28T15:44:23.425291Z  INFO osmpbudynkiv2::import::osm: Cleaning up downloaded file path=/tmp/poland-latest.osm.pbf
2026-08-28T15:44:23.564307Z  INFO osmpbudynkiv2::import::osm: OSM import complete elapsed=8m 23s
2026-08-28T15:44:23.572906Z  INFO osmpbudynkiv2::download: Downloading url="https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/OT_BUBD_A.parquet" attempt=1
Downloaded OT_BUBD_A.parquet
  [00:04:19] [========================================================================================================================================================================================================================================================================================] 1.60 GiB/1.60 GiB (6.33 MiB/s, 0s)2026-08-28T15:49:16.256773Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/OT_BUBD_A.parquet
2026-08-28T15:49:16.256791Z  INFO osmpbudynkiv2::import::bdot10k: Importing BDOT10k buildings path="/tmp/OT_BUBD_A.parquet"
2026-08-28T15:49:34.648391Z  INFO osmpbudynkiv2::import::bdot10k: Step done: load table elapsed=18.3s
2026-08-28T15:49:40.428268Z  INFO osmpbudynkiv2::import::bdot10k: Step done: create spatial indexes elapsed=5.7s
2026-08-28T15:49:40.429189Z  INFO osmpbudynkiv2::import::bdot10k: Cleaning up downloaded file path=/tmp/OT_BUBD_A.parquet
2026-08-28T15:49:40.526955Z  INFO osmpbudynkiv2::import::bdot10k: BDOT10k import complete count=16351813 elapsed=24.2s
2026-08-28T15:49:40.530209Z  INFO osmpbudynkiv2::download: Downloading url="https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet" attempt=1
Downloaded 0_budynki.parquet
  [00:06:08] [========================================================================================================================================================================================================================================================================================] 2.26 GiB/2.26 GiB (6.29 MiB/s, 0s)2026-08-28T15:56:19.286673Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/0_budynki.parquet
2026-08-28T15:56:19.286689Z  INFO osmpbudynkiv2::import::egib: Importing EGIB buildings path="/tmp/0_budynki.parquet"
2026-08-28T15:56:35.259339Z  INFO osmpbudynkiv2::import::egib: Step done: load table elapsed=15.9s
2026-08-28T15:56:41.226231Z  INFO osmpbudynkiv2::import::egib: Step done: create spatial index elapsed=5.9s
2026-08-28T15:56:41.227728Z  INFO osmpbudynkiv2::import::egib: Cleaning up downloaded file path=/tmp/0_budynki.parquet
2026-08-28T15:56:41.365309Z  INFO osmpbudynkiv2::import::egib: EGIB import complete count=17590198 elapsed=22.0s
2026-08-28T15:56:41.374096Z  INFO osmpbudynkiv2::import::prg: Downloading PRG data url="https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml"
2026-08-28T15:56:41.374123Z  INFO osmpbudynkiv2::download: Downloading url="https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml" attempt=1
Downloaded PRG-punkty_adresowe.zip
  [00:04:30] [========================================================================================================================================================================================================================================================================================] 1.67 GiB/1.67 GiB (6.32 MiB/s, 0s)2026-08-28T16:01:38.073232Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/PRG-punkty_adresowe.zip
2026-08-28T16:01:38.073247Z  INFO osmpbudynkiv2::import::prg: Preparing PRG addresses source (2021 schema) path="/tmp/PRG-punkty_adresowe.zip" teryt_source="api"
2026-08-28T16:01:38.073256Z  INFO osmpbudynkiv2::import::prg: Downloading TERC mapping from TERYT API
2026-08-28T16:01:38.076661Z  WARN rustls_platform_verifier::verification::others: Error loading CA root certificate: failed to open file: Too many levels of symbolic links (os error 40) at '/etc/pki/tls/certs/ca-certificates.crt'
2026-08-28T16:01:38.076673Z  WARN rustls_platform_verifier::verification::others: Error loading CA root certificate: failed to open file: Too many levels of symbolic links (os error 40) at '/etc/pki/tls/certs/ca-bundle.crt'
Sending request to TERYT API...
Response received.
2026-08-28T16:01:38.201643Z  INFO osmpbudynkiv2::import::prg: Step done: load TERC mapping entries=3964 elapsed=0.1s
2026-08-28T16:01:38.201771Z  INFO osmpbudynkiv2::import::prg: Found PRG 2021 GML entries in archive entries=16
2026-08-28T16:01:38.201971Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=1 of=16 zip_index=0
Building dictionaries...
2026-08-28T16:01:44.378242Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=2 of=16 zip_index=1
Building dictionaries...
2026-08-28T16:01:50.375209Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=3 of=16 zip_index=2
Building dictionaries...
2026-08-28T16:01:57.429692Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=4 of=16 zip_index=3
Building dictionaries...
2026-08-28T16:02:07.233556Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=5 of=16 zip_index=4
Building dictionaries...
2026-08-28T16:02:11.327495Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=6 of=16 zip_index=5
Building dictionaries...
2026-08-28T16:02:18.461016Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=7 of=16 zip_index=6
Building dictionaries...
2026-08-28T16:02:20.604065Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=8 of=16 zip_index=7
Building dictionaries...
2026-08-28T16:02:23.708298Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=9 of=16 zip_index=8
Building dictionaries...
2026-08-28T16:02:27.929193Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=10 of=16 zip_index=9
Building dictionaries...
2026-08-28T16:02:31.794141Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=11 of=16 zip_index=10
Building dictionaries...
2026-08-28T16:02:42.039086Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=12 of=16 zip_index=11
Building dictionaries...
2026-08-28T16:02:51.964312Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=13 of=16 zip_index=12
Building dictionaries...
2026-08-28T16:02:59.144931Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=14 of=16 zip_index=13
Building dictionaries...
2026-08-28T16:03:03.720696Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=15 of=16 zip_index=14
Building dictionaries...
2026-08-28T16:03:19.009803Z  INFO osmpbudynkiv2::import::prg: Streaming PRG GML entry entry=16 of=16 zip_index=15
Building dictionaries...
2026-08-28T16:03:21.910381Z  INFO osmpbudynkiv2::import::prg: Step done: stream PRG batches into staging table rows=8615421 elapsed=1m 43s
2026-08-28T16:03:21.910400Z  INFO osmpbudynkiv2::import::prg: Cleaning up downloaded file path=/tmp/PRG-punkty_adresowe.zip
2026-08-28T16:03:24.404974Z  INFO osmpbudynkiv2::import::prg: Step done: build prg_addresses with geom column elapsed=2.3s skipped_null_key=0 skipped_duplicate_key=0
2026-08-28T16:03:25.822476Z  INFO osmpbudynkiv2::import::prg: Step done: create spatial index elapsed=1.4s
2026-08-28T16:03:25.823182Z  INFO osmpbudynkiv2::import::prg: PRG import complete count=8615421 elapsed=6m 44s
2026-08-28T16:03:25.823722Z  INFO osmpbudynkiv2::download: Downloading url="https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/street_names_mappings.csv" attempt=1
Downloaded street_names_mappings.csv
  [00:00:00] [====================================================================================================================================================================================================================================================================================] 166.21 KiB/166.21 KiB (2.79 MiB/s, 0s)2026-08-28T16:03:26.511963Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/street_names_mappings.csv
2026-08-28T16:03:26.630298Z  INFO osmpbudynkiv2::mappings::street_names: Loaded street name mappings rows=3272 absent_from_prg=9 cells_enqueued=9096
2026-08-28T16:03:26.639708Z  INFO osmpbudynkiv2::import: Cleaning up downloaded file path=/tmp/street_names_mappings.csv
2026-08-28T16:03:26.642454Z  INFO osmpbudynkiv2::download: Downloading url="https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/bdot10k_building_types.csv" attempt=1
Downloaded bdot10k_building_types.csv
  [00:00:00] [=======================================================================================================================================================================================================================================================================================] 8.46 KiB/8.46 KiB (14.14 MiB/s, 0s)2026-08-28T16:03:26.868619Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/bdot10k_building_types.csv
2026-08-28T16:03:27.559629Z  INFO osmpbudynkiv2::mappings::building_types: Loaded building-type mappings source="bdot10k" rows=178 keys_absent_from_source=2 source_keys_uncovered=0 source_rows_uncovered=19
2026-08-28T16:03:27.562670Z  INFO osmpbudynkiv2::import: Cleaning up downloaded file path=/tmp/bdot10k_building_types.csv
2026-08-28T16:03:27.562727Z  INFO osmpbudynkiv2::download: Downloading url="https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/egib_building_types.csv" attempt=1
Downloaded egib_building_types.csv
  [00:00:00] [========================================================================================================================================================================================================================================================================================] 1.05 KiB/1.05 KiB (3.16 MiB/s, 0s)2026-08-28T16:03:27.806475Z  INFO osmpbudynkiv2::download: Download complete path=/tmp/egib_building_types.csv
2026-08-28T16:03:28.230098Z  INFO osmpbudynkiv2::mappings::building_types: Loaded building-type mappings source="egib" rows=13 keys_absent_from_source=0 source_keys_uncovered=0 source_rows_uncovered=604454
2026-08-28T16:03:28.232210Z  INFO osmpbudynkiv2::import: Cleaning up downloaded file path=/tmp/egib_building_types.csv
2026-08-28T16:03:28.260419Z  INFO osmpbudynkiv2::update::osm: Current replication sequence current_seq=7260352
2026-08-28T16:03:28.371786Z  INFO osmpbudynkiv2::update::osm: Latest available sequence latest_seq=7262612
2026-08-28T16:03:28.371798Z  INFO osmpbudynkiv2::update::osm: Sequences to apply pending=2260
OSM update complete
  [00:06:10] [============================================================================================================================================================================================================================================================================================================] 2260/2260 (0s)2026-08-28T16:09:38.772558Z  INFO osmpbudynkiv2::update::osm: OSM update complete final_seq=7262612
2026-08-28T16:09:38.775482Z  INFO osmpbudynkiv2::compare::buildings: Comparing buildings against OSM source="bdot10k"
2026-08-28T16:14:09.207249Z  INFO osmpbudynkiv2::compare::buildings: buildings comparison complete source="bdot10k" total=16016117 unmatched=485396 suppressed=4219 reported=0 matched=15526502 elapsed=4m 30s
2026-08-28T16:14:09.207267Z  INFO osmpbudynkiv2::compare::buildings: Comparing buildings against OSM source="egib"
2026-08-28T16:19:24.725936Z  INFO osmpbudynkiv2::compare::buildings: buildings comparison complete source="egib" total=17590198 unmatched=1924922 suppressed=3815 reported=0 matched=15661461 elapsed=5m 15s
2026-08-28T16:19:24.726017Z  INFO osmpbudynkiv2::compare::addresses: Comparing PRG addresses against OSM
2026-08-28T16:19:32.845972Z  INFO osmpbudynkiv2::compare::addresses: PRG comparison complete total=8615421 candidates=518237 matched=8097184 elapsed=8.1s
2026-08-28T16:26:22.412290Z  INFO osmpbudynkiv2::compare: drain complete, queue empty drained=10661

RocksDB size: 4.3 GB
DuckDB size: 8.7 GB
