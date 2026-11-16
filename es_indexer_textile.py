#!/usr/bin/env python3

from elasticsearch import ApiError, Elasticsearch, helpers
import json
import os
import re
import argparse
import sys
from datetime import datetime
from typing import Dict, Any
from tqdm import tqdm

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Note: python-dotenv not installed, using system environment variables only")

es_host = os.getenv("ELASTICSEARCH_HOST", "http://dts_dts_elasticsearch:9200")
es_username = os.getenv("ELASTICSEARCH_USERNAME")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es_verify = os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() == "true"

es_kwargs = {"verify_certs": es_verify}
if es_username and es_password:
    es_kwargs["basic_auth"] = (es_username, es_password)

# Creating an index or swapping aliases can take a while on a busy cluster: the client's 10 s default is too short
es = Elasticsearch(es_host, request_timeout=60, **es_kwargs)

INDEX = os.getenv("ELASTICSEARCH_INDEX", "documents_v5")

# Folded multi-field: lowercase + diacritics removed (français == francais, ë == e)
def _folded_text():
    return {"type": "text", "analyzer": "folded_analyzer"}


INDEX_SETTINGS = {
    "settings": {
        "index.max_ngram_diff": 4,
        "analysis": {
            "filter": {
                "ngram_filter": {
                    "type": "ngram",
                    "min_gram": 4,
                    "max_gram": 7
                }
            },
            "analyzer": {
                # Used by text search on `content`; unchanged
                "ngram_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ngram_filter"]
                },
                "standard_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                },
                # Catalog search: whole words, accent- and case-insensitive
                "folded_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"]
                },
                # Catalog search: partial words (>= 4 chars), accent- and case-insensitive
                "folded_ngram_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "ngram_filter"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "standard_analyzer",
                "fields": {
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer"
                    }
                }
            },
            "title": {
                "type": "text",
                "analyzer": "folded_analyzer",
                "fields": {
                    "ngram": {
                        "type": "text",
                        "analyzer": "folded_ngram_analyzer",
                        "search_analyzer": "folded_ngram_analyzer"
                    }
                }
            },

            "collection": { "type": "keyword" },
            "ref": { "type": "keyword" },
            "identifier": { "type": "keyword", "fields": {"text": _folded_text()} },

            "language": { "type": "keyword", "fields": {"text": _folded_text()} },
            "otherLanguage": { "type": "keyword" },

            "issued": { "type": "keyword" },
            "created": { "type": "text", "analyzer": "folded_analyzer" },
            # Parsed from `created` (e.g. "1275/1300") so years can be range-queried
            "date_start": { "type": "integer" },
            "date_end": { "type": "integer" },

            "publisher": { "type": "keyword", "fields": {"text": _folded_text()} },
            "relation": { "type": "keyword", "fields": {"text": _folded_text()} },
            "manifest_url": { "type": "keyword" },

            # Page order within its manuscript (used to export concordances in reading order)
            "position": { "type": "integer" },

            "tokens": { "type": "integer" },
            "ocr_quality": { "type": "text" }
        }
    }
}


def add_positions(docs: list) -> None:
    """Number the pages of each manuscript 0, 1, 2... in the order they appear in `docs`."""
    counters: Dict[str, int] = {}
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        key = doc.get("collection", "unknown")
        doc["position"] = counters.get(key, 0)
        counters[key] = doc["position"] + 1


def add_date_bounds(doc: Dict[str, Any]) -> None:
    """Derive integer date_start / date_end from the free-text `created` values."""
    created = doc.get("created") or []
    if not isinstance(created, list):
        created = [created]
    years = [int(y) for c in created for y in re.findall(r"\b\d{3,4}\b", str(c))]
    if years:
        doc["date_start"] = min(years)
        doc["date_end"] = max(years)


def check_elasticsearch_connection():
    try:
        info = es.info()
        print(f"Connected to Elasticsearch {info['version']['number']}")
        print(f" Cluster: {info['cluster_name']}")
        return True
    except Exception as e:
        print(f" Failed to connect to Elasticsearch: {e}")
        print(f" Trying to connect to: {os.getenv('ELASTICSEARCH_HOST', 'http://dts_dts_elasticsearch:9200')}")
        return False

def flatten_documents(data):
    flattened = []
    
    if not data:
        return flattened
    
    if isinstance(data[0], dict) and 'collection' in data[0]:
        return data
    
    for item in data:
        if isinstance(item, list):
            flattened.extend(item)
        elif isinstance(item, dict):
            flattened.append(item)
    
    return flattened

class IndexingError(Exception):
    """The new index is not good enough to go live; the live one has not been touched."""


# ---- versioned indices behind an alias --------------------------------------------------------
#
# `ELASTICSEARCH_INDEX` (e.g. documents_v5) is an ALIAS. Each run builds `documents_v5_<timestamp>`,
# checks it, then repoints the alias to it in one atomic step. Searches keep using the previous
# index until that instant, so a re-index causes no downtime, and a failed one changes nothing.

TIMESTAMP = "%Y%m%d%H%M%S"


def versioned_name(alias: str, now: datetime = None) -> str:
    return f"{alias}_{(now or datetime.now()).strftime(TIMESTAMP)}"


def _is_version_of(alias: str, name: str) -> bool:
    return re.fullmatch(re.escape(alias) + r"_\d{14}", name) is not None


def live_indices(client, alias: str) -> list:
    """The concrete indices the alias currently points to (none if it does not exist)."""
    if not client.indices.exists_alias(name=alias):
        return []
    return sorted(client.indices.get_alias(name=alias).keys())


def is_legacy_index(client, alias: str) -> bool:
    """True when `alias` is still a plain index (how this project used to be deployed)."""
    return bool(client.indices.exists(index=alias)) and not client.indices.exists_alias(name=alias)


def versions(client, alias: str) -> list:
    """Every versioned index of this alias, oldest first."""
    found = client.indices.get(index=f"{alias}_*", allow_no_indices=True, ignore_unavailable=True)
    return sorted(name for name in found if _is_version_of(alias, name))


def swap_alias(client, alias: str, new_index: str) -> list:
    """
    Point `alias` to `new_index` atomically; returns the indices it pointed to before.
    A legacy plain index named like the alias is converted in the same atomic request
    (the one case where its data is dropped: it cannot coexist with an alias of that name).
    """
    previous = live_indices(client, alias)
    actions = []
    if is_legacy_index(client, alias):
        actions.append({"remove_index": {"index": alias}})
    actions += [{"remove": {"index": old, "alias": alias}} for old in previous]
    actions.append({"add": {"index": new_index, "alias": alias}})
    client.indices.update_aliases(actions=actions)
    return previous


def prune_versions(client, alias: str, keep: int) -> list:
    """Delete the oldest versions, keeping the live one and the `keep` most recent others for rollback."""
    live = set(live_indices(client, alias))
    others = [v for v in versions(client, alias) if v not in live]
    doomed = others[:-keep] if keep > 0 else others
    for name in doomed:
        client.indices.delete(index=name)
    return doomed


def rollback(client, alias: str) -> str:
    """Point the alias back to the most recent version older than the live one; returns it."""
    live = live_indices(client, alias)
    if not live:
        raise IndexingError(f"alias {alias!r} does not exist, nothing to roll back")
    older = [v for v in versions(client, alias) if v < live[-1]]
    if not older:
        raise IndexingError("no previous version is kept; nothing to roll back to")
    swap_alias(client, alias, older[-1])
    return older[-1]


MIN_RATIO = 0.5


def check_new_index(client, new_index: str, expected: int, alias: str, force: bool = False) -> int:
    """Refuse to go live with an index that is empty, incomplete or much smaller than the current one."""
    client.indices.refresh(index=new_index)
    count = client.count(index=new_index)["count"]
    if count == 0:
        raise IndexingError(f"the new index {new_index!r} is empty")
    if count != expected:
        raise IndexingError(f"the new index holds {count} documents, expected {expected}")
    live = live_indices(client, alias) or ([alias] if is_legacy_index(client, alias) else [])
    if live and not force:
        current = client.count(index=alias)["count"]
        if current and count < current * MIN_RATIO:
            raise IndexingError(
                f"the new index has {count} documents but the live one has {current} "
                f"(less than {MIN_RATIO:.0%}); use --force if this is intended"
            )
    return count


def _doc_id(doc: dict, position: int) -> str:
    return f"{doc.get('collection', 'unknown')}_{doc.get('ref', position)}"


def create_index(index_name, client=None, wait="30s"):
    """
    Create the index and make sure it can actually take documents. When Elasticsearch cannot
    allocate its shards (most often because the disk is past a watermark) the index stays red and
    a bulk load would hang for minutes, so this fails at once with the likely reason.
    """
    client = client or es
    print(f"  Creating index '{index_name}'...")
    # Do not wait for shards here: the health check below does, once, and explains a failure
    client.indices.create(index=index_name, body=INDEX_SETTINGS, wait_for_active_shards="0")

    problem = (
        f"the new index {index_name!r} has no active shards. Check the free disk space "
        "(Elasticsearch stops allocating shards past its disk watermarks; a re-index temporarily "
        "needs room for a second copy of the index) and the cluster health"
    )
    try:
        health = client.cluster.health(index=index_name, wait_for_status="yellow", timeout=wait)
    except ApiError as error:
        if error.status_code == 408:  # Elasticsearch reports "still red after the wait" as a 408
            raise IndexingError(problem) from error
        raise
    if health.get("timed_out") or health.get("status") == "red":
        raise IndexingError(problem)


def index_documents(docs: list, index_name, client=None):
    """Bulk-index `docs` into `index_name`; returns how many were indexed. Raises if any failed."""
    client = client or es
    start_time = datetime.now()
    add_positions(docs)

    actions = []
    for i, doc in enumerate(tqdm(docs, desc="   Preparing", unit="docs")):
        if not isinstance(doc, dict):
            print(f"\n Skipping document {i} - not a dictionary: {type(doc)}")
            continue
        if "language" in doc and not isinstance(doc["language"], list):
            doc["language"] = [doc["language"]] if doc["language"] else []
        add_date_bounds(doc)
        actions.append({
            "_index": index_name,
            "_id": _doc_id(doc, i),
            "_source": doc,
        })

    indexed = 0
    with tqdm(total=len(actions), desc="   Indexing", unit="docs") as progress:
        for ok, _info in helpers.parallel_bulk(client.options(request_timeout=60), actions, chunk_size=100):
            indexed += 1 if ok else 0
            progress.update(1)
    if indexed != len(actions):
        raise IndexingError(f"only {indexed} of {len(actions)} documents were indexed")

    seconds = (datetime.now() - start_time).total_seconds()
    print(f" Indexed {indexed} documents in {seconds:.1f}s ({indexed / seconds if seconds else 0:.0f} docs/s)")
    return indexed


def test_search(index_name):
    print("\n🔍 Testing search functionality...")
    
    test_queries = [
        ("manuscript", ["content", "title"]),
        ("manu", ["title.ngram"]),
        ("francais", ["title"]),
        ("lesouef", ["title"]),
        ("lat", ["language"]),
    ]
    
    for query, fields in test_queries:
        try:
            response = es.search(
                index=index_name,
                body={
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": fields
                        }
                    },
                    "size": 3
                }
            )
            
            hits = response['hits']['total']['value']
            print(f"   Query '{query}' in {fields}: {hits} results")
            
        except Exception as e:
            print(f"   Error testing query '{query}': {e}")

def build_and_swap(docs: list, alias: str, keep: int = 1, force: bool = False, client=None) -> dict:
    """The whole zero-downtime re-index: build, check, swap, prune. Nothing live changes unless it all succeeds."""
    client = client or es
    new_index = versioned_name(alias)
    try:
        create_index(new_index, client)   # inside the try: a create that times out may still have made the index
        # documents with the same id overwrite each other, so the index holds the number of distinct ids
        expected = len({_doc_id(d, i) for i, d in enumerate(docs) if isinstance(d, dict)})
        index_documents(docs, new_index, client)
        count = check_new_index(client, new_index, expected, alias, force)
    except BaseException:
        print(f" Build failed: deleting {new_index}; the live index is untouched")
        try:
            client.indices.delete(index=new_index, ignore_unavailable=True)
        except Exception as cleanup_error:  # keep the original error; this one only leaves debris to remove by hand
            print(f" Could not delete {new_index} ({cleanup_error}); remove it manually")
        raise
    previous = swap_alias(client, alias, new_index)
    pruned = prune_versions(client, alias, keep)
    return {"new_index": new_index, "documents": count, "previous": previous, "pruned": pruned}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Index the pre-rendered pages without downtime.")
    parser.add_argument("refs_file", nargs="?", help="JSON file made by `flask data prerender-to-json`")
    parser.add_argument("index_name", nargs="?", default=INDEX, help=f"alias to publish to (default {INDEX})")
    parser.add_argument("--keep", type=int, default=1, help="previous versions to keep for rollback (default 1)")
    parser.add_argument("--force", action="store_true", help="go live even if the new index is much smaller")
    parser.add_argument("--rollback", action="store_true", help="point the alias back to the previous version")
    args = parser.parse_args(argv)
    alias = args.index_name

    print(" Elasticsearch indexing, zero-downtime (alias + versioned index)")
    print("=" * 80)
    print(f"   ELASTICSEARCH_HOST={os.getenv('ELASTICSEARCH_HOST')}  alias={alias}")
    if not check_elasticsearch_connection():
        return False

    try:
        if args.rollback:
            print(f" Rolled back: {alias} now points to {rollback(es, alias)}")
            return True

        if not args.refs_file:
            parser.error("refs_file is required unless --rollback is given")
        if not os.path.exists(args.refs_file):
            print(f" File not found: {args.refs_file}")
            return False

        with open(args.refs_file, "r") as f:
            docs = flatten_documents(json.load(f))
        print(f" Loaded {len(docs)} documents from {args.refs_file}")
        if not docs:
            print(" Nothing to index")
            return False

        before = live_indices(es, alias)
        legacy = is_legacy_index(es, alias)
        if legacy:
            print(f" '{alias}' is still a plain index: it will be replaced by an alias in the same atomic step")

        result = build_and_swap(docs, alias, keep=args.keep, force=args.force)
    except IndexingError as error:
        print(f" Not published: {error}")
        return False

    test_search(alias)
    print(f"\n Published {result['documents']} documents: {alias} -> {result['new_index']}")
    kept = [name for name in before if name not in result["pruned"]]
    if kept:
        print(f"   previous version kept for rollback: {', '.join(kept)}")
    if result["pruned"]:
        print(f"   removed old versions: {', '.join(result['pruned'])}")
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
