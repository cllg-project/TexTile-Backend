# Running these data

*From the root dir of the repository.*

## Building the catalog

```sh
PRERENDER_DIR=/home/tclerice/cache_tei FLASKAPP=app flask data catalog build \
	"./example_data/*/*.xml" \
	--mapping ./example_data/example_mapping.json \
	--external-metadata ./example_data/example_metadata.json \
	--out example_data/catalog.xml
```

## Creating the database

```sh
PRERENDER_DIR=/home/tclerice/cache_tei FLASKAPP=app flask db create
```

## Ingesting the catalog

```sh
PRERENDER_DIR=/home/tclerice/cache_tei FLASKAPP=app flask data catalog ingest \
	example_data/catalog.xml
```

## Building the pre-render (Can be long)

```sh
PRERENDER_DIR=/home/tclerice/cache_tei FLASKAPP=app flask data prerender generate \
	--media-type html \
	--workers 24
```

## Feeding the search engine