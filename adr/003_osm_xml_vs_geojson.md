# Decision
API will return GeoJSON instead of OSM XML.

# Rationale
Files that we generate need to be readable by JOSM. As far as I know JOSM can open GeoJSON files (possibly with a plugin).
Generating OSM XML files would require additional code and tests so if we don't have to have that code it's probably better to just return GeoJSON which is a standard format.
