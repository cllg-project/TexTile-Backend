#!/usr/bin/env python3

from elasticsearch import Elasticsearch, helpers
import json
import os
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

es = Elasticsearch(es_host, **es_kwargs)

INDEX = os.getenv("ELASTICSEARCH_INDEX", "documents_v5")

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
                "ngram_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ngram_filter"]
                },
                "standard_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
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
                "analyzer": "ngram_analyzer" 
            },
            
            "collection": { "type": "keyword" },
            "ref": { "type": "keyword" },
            "identifier": { "type": "keyword" },
            
            "language": { "type": "keyword" },
            "otherLanguage": { "type": "keyword" },
            
            "issued": { "type": "keyword" },
            "created": { "type": "text" },
            
            "publisher": { "type": "keyword" },
            "relation": { "type": "keyword" },
            "manifest_url": { "type": "keyword" },
            
            "tokens": { "type": "integer" },
            "ocr_quality": { "type": "text" }
        }
    }
}

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

def create_index(index_name):
    try:
        if es.indices.exists(index=index_name):
            print(f"  Index '{index_name}' already exists.")
            response = input("Do you want to delete it and recreate? (y/N): ").strip().lower()
            if response == 'y':
                print(f"  Deleting existing index '{index_name}'...")
                es.indices.delete(index=index_name)
            else:
                print(" Aborting - index already exists")
                return False
        
        print(f"  Creating index '{index_name}' with n-gram analyzer...")
        response = es.indices.create(index=index_name, body=INDEX_SETTINGS)
        print(f" Successfully created index: {response}")
        return True
    except Exception as e:
        print(f" Error creating index: {e}")
        return False

def index_documents(docs: list, index_name):
    try:
        print(f" Indexing {len(docs)} documents to '{index_name}'...")
        start_time = datetime.now()
        
        actions = []
        print("   Preparing documents for indexing...")
        for i, doc in enumerate(tqdm(docs, desc="   Preparing", unit="docs")):
            if not isinstance(doc, dict):
                print(f"\n Skipping document {i} - not a dictionary: {type(doc)}")
                continue
            
            if "language" in doc and not isinstance(doc["language"], list):
                doc["language"] = [doc["language"]] if doc["language"] else []
                
            actions.append({
                "_index": index_name,
                "_id": f"{doc.get('collection', 'unknown')}_{doc.get('ref', i)}",
                "_source": doc,
            })
        
        print(f"    Bulk indexing {len(actions)} documents...")
        es_60s = es.options(request_timeout=60)
        
        indexed_count = 0
        chunk_size = 100
        
        with tqdm(total=len(actions), desc="   Indexing", unit="docs") as pbar:
            for success, info in helpers.parallel_bulk(es_60s, actions, chunk_size=chunk_size):
                if success:
                    indexed_count += 1
                pbar.update(1)
        
        end_time = datetime.now()
        duration = end_time - start_time
        rate = len(actions) / duration.total_seconds() if duration.total_seconds() > 0 else 0
        
        print(f" Successfully indexed {indexed_count}/{len(actions)} documents in {duration}")
        print(f" Indexing rate: {rate:.1f} docs/sec")
        
        return True
    except Exception as e:
        print(f" Error during indexing: {e}")
        return False

def test_search(index_name):
    print("\n🔍 Testing search functionality...")
    
    test_queries = [
        ("manuscript", ["content", "title"]),
        ("manu", ["title.ngram"]),
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

def main():
    print(" Starting Elasticsearch indexing with metadata from catalog")
    print("=" * 80)
    
    if len(sys.argv) < 2:
        print("Usage: python es_indexer_textile.py <refs_json_file> [index_name]")
        print("\nExamples:")
        print("  python es_indexer_textile.py refs.json")
        print("  python es_indexer_textile.py refs.json documents_v6")
        print("\nNote: The refs.json file should be generated with:")
        print("  flask data prerender-to-json --out refs.json --flatten")
        sys.exit(1)
    
    refs_file = sys.argv[1]
    index_name = sys.argv[2] if len(sys.argv) > 2 else INDEX

    print(f"\n Configuration:")
    print(f"   ELASTICSEARCH_INDEX={INDEX}")
    print(f"   ELASTICSEARCH_HOST={os.getenv('ELASTICSEARCH_HOST')}")
    print(f"   ELASTICSEARCH_USERNAME={os.getenv('ELASTICSEARCH_USERNAME')}")
    print(f"   ELASTICSEARCH_PASSWORD={'***' if os.getenv('ELASTICSEARCH_PASSWORD') else None}")
    print(f"\n Using refs file: {refs_file}")
    print(f" Target index: {index_name}")

    if not os.path.exists(refs_file):
        print(f" File not found: {refs_file}")
        sys.exit(1)
    
    if not check_elasticsearch_connection():
        return False
    
    print(f"\n Loading documents from {refs_file}...")
    load_start = datetime.now()
    try:
        with open(refs_file, 'r') as f:
            docs = json.load(f)
        load_time = datetime.now() - load_start
        
        docs = flatten_documents(docs)
        
        print(f" Loaded {len(docs)} documents in {load_time}")
        
        if docs:
            print(f"\n Sample document structure:")
            sample = docs[0]
            print(f"   Keys: {list(sample.keys())}")
            print(f"   Collection: {sample.get('collection', 'N/A')}")
            print(f"   Language: {sample.get('language', 'N/A')}")
            print(f"   Tokens: {sample.get('tokens', 'N/A')}")
                    
    except Exception as e:
        print(f" Error loading refs file: {e}")
        return False
    
    print(f"\n  Setting up Elasticsearch index...")
    if not create_index(index_name):
        return False
    
    print(f"\n Indexing documents...")
    if not index_documents(docs, index_name):
        return False
    
    test_search(index_name)
    
    try:
        final_count = es.count(index=index_name)['count']
        print(f"\n Final document count in '{index_name}': {final_count}")
    except Exception as e:
        print(f" Error getting final count: {e}")
    
    print(f"\n Indexing completed successfully!")
    print(f" Index '{index_name}' is ready for use")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)