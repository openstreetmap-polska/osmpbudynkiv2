# Decision
Project will use Rust and DuckDB as backend tech stack.

# Rationale
Old app used many components which made it somewhat difficult to operate. New app should be as easy to deploy and operate as possible. That means I'm aiming to have it compiled as single binary (or a few binaries if not possible), and use a file based database/storage.

As in the original app updates to the database should follow OpenStreetMap minutely replication scripts. Frequent update would mean that some kind of embedded database system would probably be preferred over simple files.

Given that I already have a related [project](https://github.com/ttomasz/prg_convert/) for parsing address data written in Rust I think using Rust for this project would make the most sense as it should easily add this other code as dependency.

As for the database there are a few of those like SQLite (or Turso), DuckDB, ChDB. Out of those DuckDB has the best geospatial support so it would probably be the best choice.
