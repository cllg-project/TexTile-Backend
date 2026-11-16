# Running these data

*From the root dir of the repository.*

## Building the catalog

```sh
PRERENDER_DIR=/var/cache_tei FLASKAPP=app flask data catalog build \
	"./example_data/*/*.xml" \
	--mapping ./example_data/example_mapping.json \
	--external-metadata ./example_data/example_metadata.json \
	--output example_data/catalog.xml
```

## Creating the database

```sh
PRERENDER_DIR=/var/cache_tei FLASKAPP=app flask db create
```

## Ingesting the catalog

```sh
PRERENDER_DIR=/var/cache_tei FLASKAPP=app flask data catalog ingest \
	example_data/catalog.xml
```

## Building the pre-render (Can be long)

```sh
PRERENDER_DIR=/var/cache_tei FLASKAPP=app flask data prerender generate \
	--media-type html \
	--workers 24
```

## Feeding the search engine

Export the pre-rendered pages, then index them (needs a running Elasticsearch and the
`ELASTICSEARCH_*` variables from `.env`). 
```sh
PRERENDER_DIR=/var/cache_tei FLASKAPP=app flask data prerender-to-json \
	--out example_data/refs.json --workers 4

python es_indexer_textile.py example_data/refs.json
```

The indexer builds a new versioned index and swaps the `ELASTICSEARCH_INDEX` alias to it once it is complete,
so search stays up during a re-index (`--keep N` previous versions are kept; `--rollback` goes back one).
Re-run it whenever the mapping or the data changes (the mapping holds the diacritics-folded analyzers and the
`date_start` / `date_end` / `position` fields). See the backend README, "Re-indexing without downtime".

## If you are migrating to a docker instance or another disk space

```sh
PRERENDER_DIR=/var/cache_tei FLASKAPP=app flask data move-filepaths \
	/var/cache_tei/ /var/prod/cache_tei/
```
