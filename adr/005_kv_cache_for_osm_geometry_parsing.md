# Decision
Use RocksDB as KV store for node coordinates and object membership mappings.

# Rationale
At this point in time DuckDB can't do all of the operations out of core and uses too much memory.
Using some disk based cache for node coordinates might make the processing less memory hungry. 
