# osmpbudynkiv2

_ENG: Tool that prepares packages for JOSM (OpenStreetMap data editor) for easy imports of data from Polish government registries (addresses, buildings). AI rewrite of: https://github.com/openstreetmap-polska/gugik2osm_

Narzędzie do porównywania uwolnionych danych państwowych (adresy, budynki) do danych OpenStreetMap (OSM) i przygotowywania paczek danych ułatwiających dodawanie i aktualizację danych w OSM. Kontynuacja (przepisanie na nowo z użyciem AI) poprzedniej wersji: https://github.com/openstreetmap-polska/gugik2osm

## Uruchomienie

_Na tę chwilę plik wykonywalny jest budowany dla systemu Linux x86-64._

Pobierz spakowaną paczkę z githuba i rozpakuj.

Dostosuj plik konfiguracyjny.

Jeżeli chcesz dodać aplikację jako usługę systemową dostosuj też plik usługi systemd.

Jeżeli chcesz by pobieranie PRG automatycznie pobierało też słowniki TERYT (przydatne jeżeli aplikacja ma działac w trybie ciągłym na serwerze) to skonfiguruj też plik .env lub zmienne środowiskowe.

Uruchom plik wykonywalny z parametrem `init`, żeby pobrać wszystkie dane i utworzyć bazę danych (ścieżki do bazy są określone w pliku konfiguracyjnym).

```bash
osmpbudynkiv2 --config example_config.toml init
```

Gdy import się zakończy można uruchomić serwer aplikacyjny komendą:
```bash
osmpbudynkiv2 --config example_config.toml run
```

Lub ustawić usługę systemową i uruchamiać ją tak jak inne usługi w danym systemie.

Plik wykonywalny można uruchomić z flagą `--help` żeby zobaczyć wszystkie dostepne komendy.

## Kompilacja

Wymagane komponenty:
- [rustup](https://rustup.rs/) - pobierze odpowiednią wersję Rust
- Kompilator C/C++, potrzebny do skompilowania RocksDB i DuckDB
- CMake, Ninja (lub GNU Make)

Komendy do rozpoczęcia kompilacji całego projektu:
```bash
cargo build             # debug build
cargo build --release   # optimized release build
```

Pierwsza kompilacja będzie trwać dość długo ze względu na konieczność skompilowania wszystkie od zera, a RocksDB i DuckDB to spore projekty, ale kolejne kompilacje przebiegają dość szybko, bo zależności są już zbudowane i przekompilowywany jest sam kod aplikacji.

### Notatki o wersji DuckDB oraz o użyciu alokatora Jemalloc (przydatne przy aktualizacjach dependencji):

Proces budowania czyta plik `.cargo/config.toml`, który wskazuje CMake na `cmake/duckdb_version.cmake`, żeby ustawić wersję DuckDB. Kompilowanie spoza katalogu głównego repozytorium albo z już ustawioną zmienną środowiskową `CMAKE_TOOLCHAIN_FILE` pomija ten krok i daje w efekcie DuckDB, który raportuje wersję `v0.0.1` i nie potrafi zainstalować potrzebnego rozszerzenia `spatial`. Przy podnoszeniu przypiętego tagu `duckdb` trzeba zaktualizować `cmake/duckdb_version.cmake`, tak by pasował, i wymusić ponowną kompilację DuckDB poleceniem `rm -rf target/*/build/libduckdb-sys-* target/*/.fingerprint/libduckdb-sys-*` (`cargo clean -p libduckdb-sys` nie zadziała dla paczki pobranej z gita).

Oba silniki bazodanowe są budowane z jemalloc: DuckDB używa własnej, dołączonej kopii z prefiksem `duckdb_je_`, natomiast RocksDB pociąga za sobą `tikv-jemalloc-sys`, który dodatkowo podmienia `malloc` w całym procesie. Ustawienie `DUCKDB_DISABLE_JEMALLOC=1` powoduje zbudowanie DuckDB ze standardowym alokatorem. Na platformach, gdzie któraś z tych kompilacji nie obsługuje jemalloc (macOS, musl, 32-bit, BSD), odpowiednia flaga po cichu nic nie robi i używany jest standardowy alokator.

## Konfiguracja

Aplikację można skonfigurować plikiem TOML. Ścieżkę do niego podaje się flagą `--config`:

```bash
cargo run -- --config config.toml import osm
```

Jeżeli `--config` nie zostanie podane, użyte zostaną wbudowane wartości domyślne (baza w `./osmpbudynkiv2.duckdb`, poziom logowania `info` itd.). Wszystkie dostępne ustawienia wraz z wartościami domyślnymi znajdziesz w [`example_config.toml`](example_config.toml).

Plik konfiguracyjny steruje:
- **`db_path`** — położenie pliku bazy danych DuckDB
- **`rocksdb_path`** — położenie katalogu RocksDB (przechowuje surowe współrzędne węzłów OSM oraz powiązania strukturalne używane do budowania geometrii)
- **`rocksdb_block_cache_mb`** — rozmiar cache bloków RocksDB w MB (domyślnie: 512)
- **`rocksdb_write_buffer_mb`** — rozmiar bufora zapisu RocksDB w MB na rodzinę kolumn (domyślnie: 64)
- **`log_level`** — szczegółowość logów (`trace`, `debug`, `info`, `warn`, `error`)
- **`http_listen_addr`** — adres i port, na którym nasłuchuje serwer `run` (domyślnie `127.0.0.1:3000`)
- **`web_dir`** — katalog, z którego serwer `run` serwuje statyczny frontend (domyślnie `./web`). Podpięty jako trasa awaryjna (fallback), więc nigdy nie przesłania ścieżek API; brak katalogu nie jest błędem uniemożliwiającym start
- **`download_dir`** — katalog na pobierane pliki (domyślnie: systemowy katalog tymczasowy)
- **`cleanup_downloaded_files`** — usuwanie plików pobranych przez aplikację po ich przetworzeniu (domyślnie `true`)
- **`duckdb_init_commands`** — polecenia SQL wykonywane przy inicjalizacji bazy danych
- **`download_urls`** — adresy URL do pobierania źródeł danych, w tym trzech plików CSV z mapowaniami (`street_mappings`, `bdot10k_building_types`, `egib_building_types`)
- **`[teryt]`** — ustawienia słowników TERYT/TERC dla importu PRG (pobieranie albo lokalny `file_path`)
- **`[package]`** — limity endpointu `/package` (`max_area_sq_deg`, domyślnie 0.04)
- **`[updates]`** — limity okna czasowego `/updates` (`default_minutes`, `max_minutes`)
- **`[reports]`** — `POST /report` (`enabled`, domyślnie true — ustawienie false sprawia, że trasa zwraca 404; `max_objects_per_request`, domyślnie 100)
- **`[jobs.*]`** — zadania działające w tle, każde z `enabled`, `interval_seconds` oraz limitem czasu pojedynczego uruchomienia: `osm_update`, `bdot10k_update`, `egib_update`, `prg_update`, `match_refresh` (opróżnia kolejkę „brudnych” komórek, żeby tabele serwujące `*_unmatched` były aktualne; przyjmuje też `batch_size`), `match_reconcile` (okresowo dodaje do kolejki wszystkie istniejące komórki jako zabezpieczenie), `reports_reconcile` (wycofuje zgłoszenia, których rekord państwowy zmienił się, gdy zmiany nie nakładał ten proces), `street_mappings_update` i `building_types_update` (ponownie pobierają pliki CSV z mapowaniami spod `download_urls`) oraz `retention_prune` (czyści zarówno `package_exports` — `package_exports_days`, domyślnie 365 — jak i `dataset_change_areas` — `change_areas_days`, domyślnie 90). Niezależnie od tego, jak ułożą się harmonogramy, w danej chwili odświeżany jest tylko jeden zbiór danych.

Wszystkie pola są opcjonalne — wystarczy podać to, co chcesz nadpisać. Uwaga: `duckdb_init_commands` jest w całości zastępowane, jeżeli zostanie podane (nie jest scalane z wartościami domyślnymi).

### Mapowania nazw ulic

PRG publikuje skrócone nazwy ulic (`gen. Kruka`); polski OSM używa nazw
rozwiniętych (`Generała Kruka`). Plik `mappings/street_names_mappings.csv`
mapuje jedne na drugie i jest stosowany do `addr:street` przy budowaniu
odpowiedzi `/package` (oraz do podglądu tagów pokazywanego na `/tiles`
i w dymkach frontendu), dzięki czemu pobrane dane nadają się do importu bez
ręcznych poprawek.

Wczytanie:

    cargo run -- import street-mappings --file mappings/street_names_mappings.csv

Wiersz z pustym `teryt_simc_code` obowiązuje w całym kraju; wiersz z kodem
dotyczy tylko danej miejscowości i ma pierwszeństwo przed wierszem ogólnokrajowym.
Wyszukiwanie nie rozróżnia wielkości liter. Plik jest opcjonalny — bez niego
nazwy są serwowane dokładnie w takiej postaci, w jakiej publikuje je PRG.

Żeby zaproponować zmianę, edytuj plik CSV i otwórz PR; `cargo test --test
street_mappings_file` sprawdza jego strukturę.

### Mapowania typów budynków

BDOT10k i EGIB klasyfikują budynki według własnych schematów
(`budynek jednorodzinny`, `rodzaj = m`, …); pliki `mappings/bdot10k_building_types.csv`
i `mappings/egib_building_types.csv` tłumaczą je na tagi OSM
(`building=house`, `building=detached`, …). Podobnie jak mapowania nazw ulic,
stosowane są przy budowaniu odpowiedzi — na `/package`, na `/tiles`
i w dymkach obiektów we frontendzie.

Wczytanie:

    cargo run -- import building-types \
      --bdot10k-file mappings/bdot10k_building_types.csv \
      --egib-file mappings/egib_building_types.csv

Każdy wiersz ma postać `tier,key,min_levels,max_levels,max_neighbours,tags`
(plik EGIB dodaje jeszcze opisowe pole `note`). Poziom (tier) 1 dopasowuje
funkcję szczegółową (BDOT10k `PRZEWAZAJACAFUNKCJABUDYNKU`, EGIB `rodzaj_kod` —
jednoliterowa klasa wyliczana z `rodzaj` przy imporcie EGIB), poziom 2 — funkcję
ogólną (BDOT10k `FUNKCJAOGOLNABUDYNKU`; EGIB nie ma drugiego poziomu). Poziom 1
wygrywa z poziomem 2, a wśród wierszy tego samego poziomu wygrywa ten najbardziej
szczegółowy. Opcjonalne ograniczenia liczby kondygnacji i sąsiadów pozwalają
jednemu kluczowi rozstrzygać się różnie zależnie od kontekstu — wolnostojący
jedno- lub dwukondygnacyjny budynek jednorodzinny dostaje `building=detached`,
a stykający się z innym — `building=house`. Sąsiedztwo liczone jest w momencie
serwowania odpowiedzi, na podstawie pełnych tabel budynków.

**W praktyce te pliki nie są opcjonalne:** przy pustych tabelach mapowań każdy
budynek dostanie zwykłe `building=yes` i nic o tym nie ostrzeże.

Żeby zaproponować zmianę, edytuj plik CSV i otwórz PR; `cargo test --test
building_types_files` sprawdza jego strukturę. Opis tego, jak powstały te
mapowania, znajduje się w
[`docs/building_type_mappings.md`](docs/building_type_mappings.md).

## Rozwój projektu

Frontend w katalogu [`web/`](web/) (`index.html`, `app.js`, `style.css`, MapLibre
GL JS) to zwykłe pliki statyczne serwowane w czasie działania z `web_dir`, nie
wkompilowane w plik wykonywalny — ich edycja nie wymaga rekompilacji, wystarczy
przeładować stronę (twardym odświeżeniem: przeglądarka cache'uje `app.js` po
zwykłym HTTP). Frontend korzysta z `/tiles`, `/status`, `/package` i `/updates`
w obrębie tego samego origin.

```bash
cargo test              # uruchom wszystkie testy
cargo test <name>       # uruchom pojedynczy test po nazwie
cargo clippy            # lint
cargo fmt               # formatowanie kodu
```

Poziom logowania można ustawić zmienną środowiskową `RUST_LOG` (ma pierwszeństwo) albo ustawieniem `log_level` w pliku konfiguracyjnym:

```bash
RUST_LOG=debug cargo run -- import osm
cargo run -- --config config.toml import osm  # używa log_level z konfiguracji
```

### Profilowanie
```bash
samply record --save-only -o osm_import_before.json.gz \
  ./target/profiling/osmpbudynkiv2 \
  --config ./example_config.toml \
  import osm --file ./example_data/OSM/poland-latest.osm.pbf
```

Następnie `samply load osm_import_before.json.gz`, żeby przejrzeć wyniki.
