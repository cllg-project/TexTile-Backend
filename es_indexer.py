#!/usr/bin/env python3
"""
Script for first-time indexing to Elasticsearch with n-gram analyzer and metadata enrichment
This script:
1. Creates a new index with n-gram settings (same as reindex script)
2. Enriches data with metadata from counts.json
3. Indexes the enriched data to Elasticsearch

Usage: python index_enriched_to_es.py <refs_json_file> [counts_json_file]
"""

from elasticsearch import Elasticsearch, helpers
import json
import os
import sys
from datetime import datetime
from typing import Dict, Any
from tqdm import tqdm

# # Load environment variables (optional)
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except ImportError:
#     print("📝 Note: python-dotenv not installed, using system environment variables only")

# Elasticsearch client with environment variables
# Build Elasticsearch client from environment variables only. Avoid hardcoding secrets in source.
es_host = os.getenv("ELASTICSEARCH_HOST", "http://dts_dts_elasticsearch:9200")
es_username = os.getenv("ELASTICSEARCH_USERNAME")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es_verify = os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() == "true"

# Only pass basic_auth if both username and password are set. This prevents embedding secrets in code
# and allows connecting to clusters that don't require auth (e.g., in some dev setups).
es_kwargs = {"verify_certs": es_verify}
if es_username and es_password:
    es_kwargs["basic_auth"] = (es_username, es_password)

es = Elasticsearch(es_host, **es_kwargs)

# Index name from environment or default
INDEX = os.getenv("ELASTICSEARCH_INDEX", "documents_v5")

# Index settings with n-gram analyzer (same as reindex script)
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
                "standard_analyzer": {   # optional, just for clarity
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
                "analyzer": "standard_analyzer",  # default analyzer
                "fields": {
                    "ngram": {             # subfield for autocomplete/fuzzy
                        "type": "text",
                        "analyzer": "ngram_analyzer"
                    }
                }
            },
            "title": { 
                "type": "text", 
                "analyzer": "ngram_analyzer" 
            },
            
            # Metadata
            "collection": { "type": "keyword" },
            "ref": { "type": "keyword" },
            "language": { "type": "keyword" },
            "location": { "type": "text", "analyzer": "standard" },
            "filename": { "type": "keyword" },
            "ark_portail": { "type": "keyword" },
            "manifest_url": { "type": "keyword" },
            
            "tokens": { "type": "integer" },
            "start_year": { "type": "integer" },
            "stop_year": { "type": "integer" },
            
            "notes_scopecontent": { 
                "type": "text", 
                "analyzer": "ngram_analyzer" 
            },
            
            "distrib": { "type": "object" }
        }
    }
}

def check_elasticsearch_connection():
    """Check if Elasticsearch is reachable"""
    try:
        info = es.info()
        print(f"✅ Connected to Elasticsearch {info['version']['number']}")
        print(f"📍 Cluster: {info['cluster_name']}")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to Elasticsearch: {e}")
        print(f"🔧 Trying to connect to: {os.getenv('ELASTICSEARCH_HOST', 'http://dts_dts_elasticsearch:9200')}")
        return False

def load_metadata_mapping(counts_json_path: str) -> Dict[str, Dict[str, Any]]:
    """Load and index metadata by collection identifier"""
    metadata_map = {}
    
    try:
        with open(counts_json_path, 'r') as f:
            counts_data = json.load(f)
        
        print(f"   🔄 Processing {len(counts_data)} metadata entries...")
        for item in tqdm(counts_data, desc="   Loading", unit="entries"):
            qid = item.get("qid_data")
            if qid:
                metadata_map[qid] = {
                    "language": item.get("langue", ""),
                    "start_year": item.get("start_year"),
                    "stop_year": item.get("stop_year"), 
                    "location": item.get("cote", ""),
                    "ark_portail": item.get("ark_portail", ""),
                    "manifest_url": item.get("manifest_url", ""),
                    "tokens": item.get("tokens", 0),
                    "filename": item.get("filename", ""),
                    "notes_scopecontent": item.get("notes_scopecontent", ""),
                    "distrib": item.get("distrib", {})
                }
                
        print(f"✅ Loaded metadata for {len(metadata_map)} collections ({len(counts_data)} total entries)")
        return metadata_map
        
    except FileNotFoundError:
        print(f"⚠️  Metadata file {counts_json_path} not found - proceeding without enrichment")
        return {}
    except Exception as e:
        print(f"❌ Error loading metadata: {e}")
        return {}

def enrich_documents(document_lists, metadata_mapping):
    """Enrich documents with metadata from counts.json if available."""
    enriched = []
    
    print(f"   🔄 Enriching documents with metadata...")
    for doc_list in tqdm(document_lists, desc="   Enriching", unit="docs"):
        # Each item could be a list of documents or individual documents
        if isinstance(doc_list, list):
            for doc in doc_list:
                enriched_doc = enrich_single_document(doc, metadata_mapping)
                if enriched_doc:
                    enriched.append(enriched_doc)
        else:
            # Handle case where doc_list is actually a single document
            enriched_doc = enrich_single_document(doc_list, metadata_mapping)
            if enriched_doc:
                enriched.append(enriched_doc)
    
    return enriched

def enrich_single_document(doc, metadata_mapping):
    """Enrich a single document with metadata."""
    if not isinstance(doc, dict):
        print(f"Warning: Expected dict but got {type(doc)}: {doc}")
        return None
        
    # Extract collection from URL
    collection = doc.get('collection', '')
    # The collection key in metadata_mapping is the full URL, not just the ID
    collection_key = collection
    
    # Create enriched document
    enriched_doc = doc.copy()
    
    # Add metadata if available - ADD TO ROOT LEVEL, NOT NESTED
    if metadata_mapping and collection_key in metadata_mapping:
        metadata = metadata_mapping[collection_key]
        
        # Add all metadata fields directly to the document root
        enriched_doc.update(metadata)
        
        print(f"✅ Enriched document for collection {collection_key}")
    else:
        print(f"⚠️  No metadata found for collection: {collection_key}")
    
    return enriched_doc

def create_index(index_name):
    """Create new index with n-gram settings"""
    try:
        # Check if index already exists
        if es.indices.exists(index=index_name):
            print(f"⚠️  Index '{index_name}' already exists.")
            response = input("Do you want to delete it and recreate? (y/N): ").strip().lower()
            if response == 'y':
                print(f"🗑️  Deleting existing index '{index_name}'...")
                es.indices.delete(index=index_name)
            else:
                print("❌ Aborting - index already exists")
                return False
        
        # Create new index
        print(f"🏗️  Creating index '{index_name}' with n-gram analyzer...")
        response = es.indices.create(index=index_name, body=INDEX_SETTINGS)
        print(f"✅ Successfully created index: {response}")
        return True
    except Exception as e:
        print(f"❌ Error creating index: {e}")
        return False

def index_documents(docs: list, index_name):
    """Index enriched documents to Elasticsearch"""
    try:
        print(f"📥 Indexing {len(docs)} documents to '{index_name}'...")
        start_time = datetime.now()
        
        # Prepare actions with progress bar
        actions = []
        print("   🔄 Preparing documents for indexing...")
        for i, doc in enumerate(tqdm(docs, desc="   Preparing", unit="docs")):
            if not isinstance(doc, dict):
                print(f"\n⚠️  Skipping document {i} - not a dictionary: {type(doc)}")
                continue
                
            actions.append({
                "_index": index_name,
                "_id": f"{doc.get('collection', 'unknown')}_{doc.get('ref', i)}",
                "_source": doc,
            })
        
        # Bulk index with extended timeout and progress tracking
        print(f"   📤 Bulk indexing {len(actions)} documents...")
        es_60s = es.options(request_timeout=60)
        
        # Track progress during bulk indexing
        indexed_count = 0
        chunk_size = 100
        total_chunks = (len(actions) + chunk_size - 1) // chunk_size
        
        with tqdm(total=len(actions), desc="   Indexing", unit="docs") as pbar:
            for success, info in helpers.parallel_bulk(es_60s, actions, chunk_size=chunk_size):
                if success:
                    indexed_count += 1
                pbar.update(1)
        
        end_time = datetime.now()
        duration = end_time - start_time
        rate = len(actions) / duration.total_seconds() if duration.total_seconds() > 0 else 0
        
        print(f"✅ Successfully indexed {indexed_count}/{len(actions)} documents in {duration}")
        print(f"� Indexing rate: {rate:.1f} docs/sec")
        
        return True
    except Exception as e:
        print(f"❌ Error during indexing: {e}")
        return False

def test_search(index_name):
    """Test search functionality on the new index"""
    print("\n🔍 Testing search functionality...")
    
    test_queries = [
        "manuscript",
        "manu",  # Test n-gram
        "medieval", 
        "med"    # Test n-gram
    ]
    
    for query in test_queries:
        try:
            response = es.search(
                index=index_name,
                body={
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": ["content", "title", "notes_scopecontent"]
                        }
                    },
                    "size": 3
                }
            )
            
            hits = response['hits']['total']['value']
            print(f"   Query '{query}': {hits} results")
            
        except Exception as e:
            print(f"   ❌ Error testing query '{query}': {e}")

def main():
    """Main indexing process with metadata enrichment"""
    print("🚀 Starting Elasticsearch indexing with n-gram analyzer and metadata enrichment")
    print("=" * 80)
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python es_indexer.py <refs_json_file> [counts_json_file] [index_name]")
        print("\nExamples:")
        print("  python es_indexer.py refs.json")
        print("  python es_indexer.py refs.json counts.json")
        print("  python es_indexer.py refs.json counts.json documents_v6")
        sys.exit(1)
    
    refs_file = sys.argv[1]
    counts_file = sys.argv[2] if len(sys.argv) > 2 else "counts.json"
    index_name = sys.argv[3] if len(sys.argv) > 3 else INDEX

    # Print used environment variables and files
    print(f"\n🔧 Using environment variables:")
    print(f"   ELASTICSEARCH_INDEX={INDEX}")
    print(f"   ELASTICSEARCH_HOST={os.getenv('ELASTICSEARCH_HOST')}")
    print(f"   ELASTICSEARCH_USERNAME={os.getenv('ELASTICSEARCH_USERNAME')}")
    print(f"   ELASTICSEARCH_PASSWORD={'***' if os.getenv('ELASTICSEARCH_PASSWORD') else None}")
    print(f"\n📂 Using refs file: {refs_file}")
    print(f"📂 Using metadata file: {counts_file}")
    print(f"🎯 Target index: {index_name}")

    # Verify refs file exists
    if not os.path.exists(refs_file):
        print(f"❌ File not found: {refs_file}")
        sys.exit(1)
    
    # Step 1: Check Elasticsearch connection
    if not check_elasticsearch_connection():
        return False
    
    # Step 2: Load data files
    print(f"\n📖 Loading documents from {refs_file}...")
    load_start = datetime.now()
    try:
        with open(refs_file, 'r') as f:
            docs = json.load(f)
        load_time = datetime.now() - load_start
        print(f"✅ Loaded {len(docs)} documents in {load_time}")
        
        # Debug: check structure of first few documents
        if docs:
            print(f"📋 Document structure check:")
            for i, doc in enumerate(docs[:3]):  # Check first 3 documents
                print(f"   Doc {i}: type={type(doc)}")
                if isinstance(doc, dict):
                    keys = list(doc.keys())[:5]  # Show first 5 keys
                    print(f"   Doc {i}: keys={keys}")
                elif isinstance(doc, list):
                    print(f"   Doc {i}: list with {len(doc)} items")
                    if doc:
                        print(f"   Doc {i}: first item type={type(doc[0])}")
                else:
                    print(f"   Doc {i}: content={str(doc)[:100]}...")
                    
    except Exception as e:
        print(f"❌ Error loading refs file: {e}")
        return False
    
    print(f"\n🔄 Loading metadata from {counts_file}...")
    meta_start = datetime.now()
    metadata_map = load_metadata_mapping(counts_file)
    meta_time = datetime.now() - meta_start
    print(f"⏱️  Metadata loading took {meta_time}")
    
    # Step 3: Enrich documents with metadata
    print(f"\n✨ Enriching documents with metadata...")
    enrich_start = datetime.now()
    enriched_docs = enrich_documents(docs, metadata_map)
    enrich_time = datetime.now() - enrich_start
    enriched_count = len(enriched_docs)
    print(f"⏱️  Document enrichment took {enrich_time}")
    
    # Step 4: Create index
    print(f"\n🏗️  Setting up Elasticsearch index...")
    if not create_index(index_name):
        return False
    
    # Step 5: Index documents
    print(f"\n📥 Indexing documents...")
    if not index_documents(enriched_docs, index_name):
        return False
    
    # Step 6: Test search functionality
    test_search(index_name)
    
    # Step 7: Final verification
    try:
        final_count = es.count(index=index_name)['count']
        print(f"\n📊 Final document count in '{index_name}': {final_count}")
    except Exception as e:
        print(f"❌ Error getting final count: {e}")
    
    print(f"\n✅ Indexing completed successfully!")
    print(f"🎯 Enhanced index '{index_name}' is ready for use")
    print(f"📈 Documents enriched: {enriched_count}/{len(docs)}")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)