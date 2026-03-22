# Decision
Rewrite the app available under: https://budynki.openstreetmap.org.pl instead of trying to fix old code.

# Rationale
Previous version of [the app](https://github.com/openstreetmap-polska/gugik2osm) stopped working over time from lack of maintenance. Additionally there were changes to external data that is used which require rewriting ingestion code and probably rethinking assumptions that were made when the app was initially built.

Given the amount of technical debt it's better to rewrite it from scratch.

When writing new version we'll need to re-asses assumptions on which data sets to use and how to compare them.
