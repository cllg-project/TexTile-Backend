# Migrating the backend: `0a97986` → this release

For a deployment running the previous release (commit `0a97986`, "Merge pull request #8"). The
frontend has its own, shorter guide (`MIGRATION.md` in the frontend repository, from `1785c10`).

`migrate.py` automates the checks and the one step that changes Elasticsearch:

```bash
python migrate.py check [--refs refs.json]     # read-only pre-flight
python migrate.py reindex refs.json            # build the new index, swap the alias (no downtime)
python migrate.py verify --base-url URL        # smoke-test the running API afterwards
python migrate.py rollback                     # point the alias back to the previous index version
```

## In short

| # | Step | Downtime |
|---|------|----------|
| 1 | Back up, check disk space (`migrate.py check`) | none |
| 2 | Install the new dependencies / build the new image | none |
| 3 | **Re-index** into a new index behind an alias (`migrate.py reindex`) | none: the old API keeps serving |
| 4 | **Deploy the new API** (note: the image no longer contains your `.env`) | a restart |
| 5 | `migrate.py verify` | none |
| 6 | Deploy the new frontend | none |

**The order matters, and it was tested:** the new API sorts on fields that only the new index has, so
the new API against an old index answers HTTP 400 (`No mapping found for [date_start] in order to sort
on`). The reverse is safe: the queries of the previous API (its catalog search and its text search) run
unchanged against the new index and return the same results (tested directly against Elasticsearch). So
re-index first, deploy the API second, the frontend last (it calls endpoints the old API does not have).

The SQLite database (`app.db`), the catalog format and the pre-render cache **did not change**: there
is nothing to migrate or rebuild there.

---

## 1. Before you start

**Disk space.** The re-index builds a *second copy* of the index next to the live one. Elasticsearch
stops allocating shards for new indices once the disk passes its high watermark (90% by default), and
then the new index stays red. `migrate.py check` computes it for you:

```
✗ disk space — already past the 90% watermark: 147.6 GiB free of 1705.5 GiB ...
```

Free some space first; do not raise the watermark on a shared cluster to get around it.

**Backups** (all optional, none of the steps below touches `app.db`):

```bash
cp app.db app.db.pre-migration
# An exact copy of the old index, in case you want to compare. It costs disk (a third copy), and its
# name does not match documents_v5_<timestamp>, so the indexer never prunes it.
curl -XPOST localhost:9200/_reindex -H 'content-type: application/json' \
  -d '{"source":{"index":"documents_v5"},"dest":{"index":"documents_v5_backup_pre_migration"}}'
```

**The data to index.** You need the JSON export of the pre-rendered pages. If you do not have a recent
one: `flask data prerender-to-json --out refs.json --workers 4` (same command as before).

## 2. Install

```bash
git pull                                   # or check out the release
pip install -r requirements.txt            # dapytains is now pinned to 1.0.0rc4 (it was unpinned)
pip install -r requirements-dev.txt        # only to run the tests: pytest
python migrate.py check --refs refs.json
```

`check` must show no ✗. Warnings are explained in the output (a plain index to convert, an unset
`PUBLIC_BASE_URL`...).

## 3. Re-index (no downtime)

```bash
python migrate.py reindex refs.json
```

What happens, and why it is safe:

1. A new index `documents_v5_<timestamp>` is created with the new mapping and filled. The live index
   is not touched and keeps serving. (2,919 pages take about 2 s on a laptop; scale by your corpus.)
2. It is checked: it must exist with active shards, hold every document, and not be less than half the
   size of the live index (`--force` to override that last check).
3. In **one atomic request** the name `documents_v5` is turned into an alias of the new index. On a
   deployment still running the previous release `documents_v5` is a *plain index*: it is replaced by the
   alias in that same request. That plain index is then gone, which is why step 1 suggests a backup.
4. Older versions beyond `--keep N` (default 1) are deleted.

If anything fails before step 3, the half-built index is deleted and nothing live has changed.
`ELASTICSEARCH_INDEX` keeps its value (`documents_v5`): it is now an alias, and the application,
point-in-time searches and the indexer all use it by that name.

If the new index cannot get shards you will see (after 30 s):

```
Not published: the new index '...' has no active shards. Check the free disk space ...
```

## 4. Deploy the new API

### Docker: the image no longer contains your `.env` (breaking)

`Dockerfile.api` used to `COPY .env` (and a `variants.csv` that was not in the repository) into the
image, which put credentials in it. It no longer does. Provide the configuration when you run it:

```bash
docker build -f Dockerfile.api -t textile-api .
docker run --env-file .env -p 5000:5000 textile-api
```

```yaml
# docker-compose.yml
services:
  api:
    build: { context: ., dockerfile: Dockerfile.api }
    env_file: .env
```

The container now starts `gunicorn -c gunicorn.conf.py app:app`: threaded workers (up to 4 × 8
threads), `info` logging instead of `debug`, 120 s timeout, workers recycled periodically. Override with
`WEB_CONCURRENCY`, `GUNICORN_THREADS`, `GUNICORN_TIMEOUT`, `LOG_LEVEL`, `BIND`. The health check and the
port (5000) are unchanged.

### New configuration (all optional; defaults shown)

| Variable | Purpose | Default |
|---|---|---|
| `PUBLIC_BASE_URL` | Public address of the API: **every link in API answers is built from it**. Set it behind a reverse proxy, e.g. `https://example.org/dts/` | links follow each request |
| `TRUSTED_PROXY_HOPS` | Alternative: trust `X-Forwarded-*` from this many proxies (ignored when `PUBLIC_BASE_URL` is set) | `0` |
| `STATS_MAX_OCCURRENCES` | Most occurrences one statistics scan counts (exact) | `100000` |
| `STATS_MAX_SECONDS` | Time budget of one statistics scan | `60` |
| `STATS_MAX_CONCURRENT` / `EXPORT_MAX_CONCURRENT` | Heavy requests at once, per worker (more get a 503 + `Retry-After`) | `2` / `4` |
| `CONCORDANCE_MAX_ROWS` | Most rows of a concordance CSV export | `100000` |
| `API_CACHE_DIR` | Saved statistics, shared by the workers | `<PRERENDER_DIR>/api-cache` |
| `API_CACHE_TTL`, `API_CACHE_MAX_ENTRIES` | Their lifetime (s) and count | `86400`, `2000` |
| `HIERARCHY_CACHE_SECONDS` | How long the collection hierarchy is kept in memory | `60` |
| `PAGINATION_SIZE` | Members per page of `/collection/` (now parsed as a number; as a string it broke paging) | `20` |

Make sure `API_CACHE_DIR` is writable by the API user; if it is not, results are simply not cached.

### Behind a reverse proxy

Set `PUBLIC_BASE_URL` to the address users reach the API at. The proxy may strip a path prefix or keep it.
The frontend's `VITE_DTS_BASE_URL` must be that same address. Add the frontend's origin to
`ALLOWED_ORIGINS` as before.

## 5. Verify

```bash
python migrate.py verify --base-url http://localhost:5000 \
    --origin https://your-frontend.example.org \
    --expect-public-url https://example.org/dts \
    --text-query et          # any word that occurs in your texts
```

All lines must be ✓. In particular `DTS entry point (1.0, with page)` tells you the new code is the
one running, and `catalog facets (new mapping)` that the new index is the one being served.

## 6. Deploy the frontend

See the frontend `MIGRATION.md`.

---

## What changed for API consumers

**DTS answers** (`/`, `/collection/`, `/navigation/`, `/document/`)

| Before | Now |
|---|---|
| `@context` `…/specifications/context/1-alpha1.json`, `dtsVersion` `1-alpha` on `/collection/` | `https://dtsapi.org/context/v1.0.json`, `1.0` (taken from dapytains, on every answer) |
| collection template `…/collection/{?id}{&nav}` | `…/collection/{?id}{&page,nav}` (`page` is listed because `/collection/` paginates) |
| pagination links (`view.first`…) relative: `/collection/?id=…` | absolute, built from the public address |
| `page` past the last page: `200` with no members | `404` `{"message": "Page N does not exist, the last page is M"}` |
| member `nb_children` (top level) | `member.extensions.nb_children` (an array, like every `extensions` value). **Clients reading `nb_children` must change.** |

**Catalog and search**

- `GET /manuscripts/`: every word of `q` must match; accents and case are ignored (`francais` = `français`);
  short numbers also match longer ones (`19` → `19`, `190`, `119`, exact first); a year matches manuscripts
  whose date range contains it. New filters `language`, `start_year`, `stop_year`, `within=<collection id>`.
  One result per manuscript, paged by Elasticsearch (the old 1000-manuscript cap is gone); `title` is the
  whole highlighted title; `created` and `language` can be lists.
- Legacy endpoints are kept, now thin wrappers: `/manuscripts/language/` matches whole codes only (it used to
  match substrings); `/manuscripts/date/` requires *all* given parameters to match (it used to match any);
  `/manuscripts/range/` no longer returns a 500.
- `/search/?format=csv` is now the concordance export. Its columns changed, from
  `collection,ref,title,coverage,content_line` (it returned only the header row) to
  `collection,manuscript,ref,line,left,keyword,right,language,date`.
- `url` fields of `/collections/list/` and `/manuscripts/list/` include the path prefix.

**New endpoints**: `/collections/search/`, `/manuscripts/facets/`, `/manuscripts/list/`,
`/concordance/` (CSV or `format=json`), `/concordance/stats/` (`json`, `csv`, or streamed `ndjson`).
See the README.

**Heavy requests** can answer `503` with a `Retry-After` header when the server is busy.

## What changed in Elasticsearch

| Field | Before | Now |
|---|---|---|
| `title` | `text`, n-gram analyzer | `text`, lowercase + accent-folding analyzer, with `title.ngram` (folded n-grams) |
| `created` | `text`, standard | `text`, accent-folding |
| `language`, `publisher`, `identifier`, `relation` | `keyword` | `keyword` + `.text` sub-field (folded) |
| `date_start`, `date_end` | — | `integer`, derived from `created` (`"1275/1300"`) |
| `position` | — | `integer`, page order inside its manuscript (concordances come out in reading order) |
| `content` | unchanged | unchanged |
| index name | a plain index `documents_v5` | alias `documents_v5` → `documents_v5_<timestamp>` |

## Rollback

- **Code:** redeploy the previous image. Its queries work on the new index (see "The order matters" above), so this
  is safe at any time.
- **Index:** `python migrate.py rollback` points the alias back to the previous version created by the new
  indexer (`--keep` controls how many are kept). It deletes nothing. The *plain* index of the previous release is
  not a version: it is gone after the first re-index (hence the optional backup in step 1).
- **Saved statistics:** delete the `api-cache` directory at any time; they are recomputed on demand. They are
  also invalidated by themselves whenever the index changes.
- **Frontend:** redeploy the previous build. Against the new API its collection tree shows `0` child counts
  (it reads the old top-level `nb_children`); everything else keeps working.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `check`: `disk space — already past the 90% watermark` | Free disk space (see step 1). |
| `reindex`: `Not published: … no active shards` | Same cause usually; also check `curl localhost:9200/_cluster/health`. Nothing live changed. |
| `reindex`: `… less than 50% …` | The new index is much smaller than the live one: check the refs file, or `--force` if intended. |
| API: HTTP 400 `No mapping found for [date_start]` on `/manuscripts/` or `/concordance/` | The new API is running against the old index: run step 3. |
| `verify`: `@context is …1-alpha1…` | The previous code is still the one serving requests. |
| `verify`: `a repeated request was not served from the cache` | `API_CACHE_DIR` is not writable. |
| `503` from `/concordance/…` | More heavy requests than `STATS_MAX_CONCURRENT` / `EXPORT_MAX_CONCURRENT`; retry after the delay. |
| Links in answers point to `localhost:5000` or the internal host | Set `PUBLIC_BASE_URL`. |
| Browser: CORS error | Add the frontend origin to `ALLOWED_ORIGINS` (exact scheme, host and port). |
