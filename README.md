# Textile API

A Flask-based API for searching and accessing medieval manuscript collections, providing advanced text search capabilities over TEI-encoded documents.

## ✨ Features

- **Full-text search** across manuscript collections with highlighting
- **Manuscript catalog search** with metadata filtering (date, language, location)
- **Hybrid search** combining traditional text matching with vector similarity
- **Date range queries** (e.g., "800-1400" for medieval periods)
- **Export capabilities** (CSV format for research data)
- **TEI document processing** and rendering
- **Elasticsearch integration** for high-performance search
- **Caching system** for improved response times



## 📚 API Endpoints

### Text Search
```http
GET /search/?q=<query>&page=1&size=25&mode=exact
```
- **Modes**: `exact`, `fuzzy`, `partial`
- **Export**: Add `format=csv` for CSV download

### Concordance (CSV export)
```http
GET /concordance/?q=<query>&mode=exact&language=lat&resource=<manuscript id>&within=<collection id>&width=40&limit=100000
```
Keyword in context for every occurrence matching a text search: same query syntax, modes and filters as
`/search/` (`/search/?format=csv` is the same export). One row per occurrence, in reading order
(manuscript, page order, position in the page):

`collection, manuscript, ref, line, left, keyword, right, language, date`

- **Streamed**: rows are written while Elasticsearch is paged through (point in time + `search_after`),
  so a concordance of tens of thousands of lines starts downloading at once and uses constant memory.
  The `X-Matching-Pages` header gives the number of matching pages up front.
- `width` is the context in characters on each side (5–200, default 40). `limit` is capped by the
  `CONCORDANCE_MAX_ROWS` setting (default 100000).
- A quoted phrase is one keyword (`"la bouche"` → `la bouche`). Line breaks in the context become spaces.
- UTF-8 with a BOM, so spreadsheets read the diacritics correctly. Cells starting with `= + - @` are prefixed
  with `'` so a spreadsheet never runs them as formulas.
- Reading order needs the `position` field: **re-index** (`es_indexer_textile.py`) after upgrading.

#### Viewing the concordance and its statistics

```http
# One page of the concordance as JSON (the text search page's "Concordance" tab)
GET /concordance/?q=<query>&format=json&page=1&size=20&width=40

# Frequency statistics of the same search (its "Statistics" tab); add &format=csv to download them
GET /concordance/stats/?q=<query>&mode=&language=&resource=&within=
```
- The JSON view pages through the *matching pages* (`size` per page, up to 100), each giving all its
  occurrences, and reports `total_pages` and `pages`. Elasticsearch's 10,000-result window applies;
  the CSV export has no such limit.
- The statistics scan every matching page (same query, modes and scope as the concordance, so the
  numbers agree with the CSV) and return: `occurrences`, `per10k` (occurrences per 10,000 tokens of
  *all* manuscripts in the scope, with or without hits), the distinct `forms` that matched (what a
  wildcard, fuzzy or `deus,deum` search is made of), and the distribution `by_manuscript`, `by_century`
  (century of the middle of the date range; centuries without hits are listed with 0) and `by_language`.
- A manuscript whose token count is unknown has a `null` relative frequency, never 0.
- **Streaming**: `&format=ndjson` sends one JSON object per line while the scan runs: `start` (with the
  total number of matching pages), then `progress` events about twice a second (pages scanned and the
  figures so far), then `done` (or `error`). The frontend uses it to show a loading indicator and fill
  the figures in as they arrive. Closing the connection stops the scan on the server.
- **Limits**: at most `STATS_MAX_OCCURRENCES` (100,000) occurrences are counted, exactly, and at most
  `STATS_MAX_SECONDS` seconds are spent. The final result says if it stopped early (`truncated`,
  `truncated_reason`: `occurrences` or `time`, plus `limit`). `&limit=` can lower the maximum, not raise it.
  At most `STATS_MAX_CONCURRENT` scans (and `EXPORT_MAX_CONCURRENT` CSV exports) run at once per worker;
  a request over that gets `503` with a `Retry-After` header.
- **Cache**: a finished result is saved on disk, keyed by the search and the index generation, so it is
  served at once next time and a re-index invalidates it. `&refresh=1` recomputes. A scan cut short by
  the time limit is not saved. Results say `cached: true|false`.
- Neighbouring hits are merged into one keyword only for a quoted multi-word phrase; for `deus,deum`,
  `deus deum` is two occurrences.

### Manuscript Catalog
```http
# Catalog search: free text, plus optional filters (all combine)
GET /manuscripts/?q=<query>&language=<code>&start_year=<y>&stop_year=<y>&within=<collection id>&page=1&size=25

# Collections (any depth), diacritics/case-insensitive; `within` limits to a sub-tree
GET /collections/search/?q=<query>&within=<collection id>&page=1&size=20

# Values for the filters: languages (with counts) and the date span
GET /manuscripts/facets/
```
- Every whitespace-separated term of `q` must match (`smith 19` finds *Smith-Lesouëf 19*), accents
  and case are ignored (`francais` = `français`), and short numbers also match longer ones
  (`19` finds `190`, ranked after the exact `19`). A year (`1290`) matches manuscripts whose date range contains it.
- Results are one entry per manuscript, paginated by Elasticsearch (`total` counts manuscripts).
- Legacy endpoints, kept for compatibility and implemented on top of the same code:
  `/manuscripts/language/?q=`, `/manuscripts/range/?q=800-1400`,
  `/manuscripts/date/?start_year=&stop_year=&exact_start=&exact_stop=`, `/manuscripts/list/`.

### Statistics
```http
# Get total manuscript count
GET /manuscripts/count/

# List all collections
GET /collections/list/
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ELASTICSEARCH_HOST` | Elasticsearch connection URL | `http://dts_elasticsearch:9200` |
| `ELASTICSEARCH_INDEX` | Index name for documents | `documents` |
| `PUBLIC_BASE_URL` | Public address of the API; every link in API responses is generated from it (see below) | *(request address)* |
| `TRUSTED_PROXY_HOPS` | Number of reverse proxies whose `X-Forwarded-*` headers to trust, when `PUBLIC_BASE_URL` is not set | `0` |
| `STATS_MAX_OCCURRENCES` | Most occurrences a statistics scan counts (exact; the result says when it stopped there) | `100000` |
| `STATS_MAX_SECONDS` | Time budget of one statistics scan | `60` |
| `STATS_MAX_CONCURRENT` | Statistics scans running at once, per worker; more get a 503 | `2` |
| `EXPORT_MAX_CONCURRENT` | CSV exports running at once, per worker; more get a 503 | `4` |
| `API_CACHE_DIR` | Where finished statistics are cached on disk (shared by the workers) | `<PRERENDER_DIR>/api-cache` |
| `API_CACHE_TTL` / `API_CACHE_MAX_ENTRIES` | Lifetime (seconds) and size of that cache | `86400` / `2000` |
| `HIERARCHY_CACHE_SECONDS` | How long the collection hierarchy is kept in memory | `60` |
| `WEB_CONCURRENCY`, `GUNICORN_THREADS`, `GUNICORN_TIMEOUT`, `LOG_LEVEL`, `BIND` | gunicorn workers, threads per worker, timeout, log level, address (see `gunicorn.conf.py`) | `min(4, 2×CPU+1)`, `8`, `120`, `info`, `0.0.0.0:5000` |
| `CONCORDANCE_MAX_ROWS` | Upper bound on the rows of one concordance export | `100000` |
| `PAGINATION_SIZE` | Members per page on `/collection/` | `20` |
| `USE_DISK_CACHE` | Enable file-based caching | `true` |
| `PRERENDER_DIR` | Cache directory path | `/app/cache` |
| `DATABASE_PATH` | SQLite database location | `/app/app.db` |


### Re-indexing without downtime

`ELASTICSEARCH_INDEX` (e.g. `documents_v5`) is an **alias**. `es_indexer_textile.py` builds a fresh
index named `documents_v5_<timestamp>`, checks it, and only then repoints the alias in one atomic step,
so searches keep using the old index until the instant of the swap. A failed build changes nothing.

```bash
python es_indexer_textile.py example_data/refs.json            # build, check, swap
python es_indexer_textile.py example_data/refs.json --keep 2   # keep two previous versions
python es_indexer_textile.py --rollback                        # point the alias back to the previous version
```

- The new index must hold every document, and must not be less than half the size of the live one
  (`--force` overrides that second check).
- The first run on a deployment that still has a plain `documents_v5` index converts it to an alias in the
  same atomic request; that plain index is then gone, so run it once with the data at hand.
- A re-index temporarily needs room for a **second copy** of the index. If Elasticsearch cannot allocate the
  new index's shards (usually a disk watermark), the indexer stops after 30 s, says so, and removes it.
- The statistics cache and the facets are keyed by the index generation, so they refresh by themselves.

### Running with gunicorn

`gunicorn -c gunicorn.conf.py app:app` (what the image runs). It uses threaded workers, so a long statistics
stream or CSV export occupies one thread instead of freezing the worker, a 120 s timeout (a heartbeat for
threaded workers, not a cap on request length), `info` logging, and recycles workers periodically.
Concurrency is `WEB_CONCURRENCY` workers × `GUNICORN_THREADS` threads; the heavy-request limits above
are per worker.

### Links behind a reverse proxy

The DTS responses contain links (entry point, `collection` / `document` / `navigation` URI
templates, pagination). They are built from the address the API is reached at, so behind a proxy
they would point to the internal address. Set `PUBLIC_BASE_URL` to the public one:

```bash
PUBLIC_BASE_URL=https://example.org/dts/   # served under a path prefix
```

The proxy may either strip the `/dts` prefix or keep it; both work. The frontend's
`VITE_DTS_BASE_URL` should be that same address. If your proxy sets `X-Forwarded-Proto/Host/Prefix`
you can use `TRUSTED_PROXY_HOPS=1` instead.

## 🐳 Docker

The image does not contain any configuration. Pass it at runtime:

```bash
docker build -f Dockerfile.api -t textile-api .
docker run --env-file .env -p 5000:5000 textile-api
```

## 🛠 Development

### Local Development

1. **Set up Python environment:**
   ```bash
   python -m venv env
   source env/bin/activate
   pip install -r requirements.txt
   ```

2. **Run Flask development server:**
   ```bash
   export FLASK_APP=app
   flask run
   ```

3. **Run the tests:**
   ```bash
   pip install -r requirements-dev.txt
   pytest
   ```

### Key Dependencies

- **Flask** - Web framework
- **Elasticsearch 8.11.0** - Search engine
- **MyDapytains** (pinned to `1.0.0rc4`, the latest release) - Python implementation of the Distributed Text Services (DTS) API. See https://github.com/distributed-text-services/MyDapytains for details.
- **SaxonCHE** - XSL transformations for TEI
- **BeautifulSoup4** - HTML/XML parsing

## 📡 DTS API

Answers follow **DTS 1.0** as produced by dapytains `1.0.0rc4`: `@context` is
`https://dtsapi.org/context/v1.0.json` and `dtsVersion` is `1.0` (both taken from the library, not hardcoded).
Differences from the library's own routes, all additions:

- `/collection/` is **paginated** (`page`, 20 members by default, a `view` object of type `Pagination` with
  absolute `first` / `previous` / `next` / `last` links) and sortable (`sort_by=title|nb_children`, `sort_order`).
  The collection URI template therefore advertises `page`: `collection/{?id}{&page,nav}`
  (the same template as the specification's `{?id,page,nav}`, written so partially expanded links keep `{&page,nav}`).
  A page past the last one is a 404.
- Our number of children per member is in `extensions.nb_children` (an array, like every `extensions` value),
  which is where the specification puts custom properties.



- Entry point
```http
GET /
```
Returns JSON-LD entrypoint with URITemplates for collection, navigation and document resources.

- Collection view
```http
GET /collection/?id=<collection_id>&nav=<children|parents>
```
Parameters: `id` (optional; when omitted returns the top-level/root collection), `nav` (optional, `children` by default).

- Navigation view
```http
GET /navigation/?resource=<collection_id>&ref=<ref>&start=<start>&end=<end>&tree=<tree>&down=<int>
```
Parameters: `resource` (required), provide either `ref` or `start`+`end` to select a citable unit or range; `tree` picks the citation tree; `down` controls depth (integer). Returns JSON(-LD) Navigation responses.

- Document view
```http
GET /document/?resource=<collection_id>&ref=<ref>&start=<start>&end=<end>&tree=<tree>&mediaType=<mime-type>
```
Parameters: `resource` (required), `ref` or `start`+`end`, optional `tree`. `mediaType` requests a transformed representation when supported (default returns TEI XML).
