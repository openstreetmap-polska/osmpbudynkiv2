# Decision
Data that is imported should be standardized on EPSG:4326.

# Rationale
It will be simpler to have all imported data in single CRS. Otherwise comparisons between datasets will require dynamic reprojection/transformation and that would complicate the code.
Main question is which CRS to pick from valid options like: EPSG:4326 (latitude, longitude in degrees), EPSG:2180 (native to Poland, high accuracy, in meters), EPSG:3857 (used for web map, in meters).
First option seems the most universal even though for distance comparisons which we'll do it will require specific functions that internally transform it to crs using meters.
