#!/usr/bin/env python
import os
import sys
import json
from flask import Flask
from app import app, db
from dapytains.app.database import Collection


def load_manifest_mapping(mapping_path: str) -> dict:
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
        print(f"✅ Loaded {len(mapping)} mappings from {mapping_path}")
        return mapping
    except Exception as e:
        print(f"❌ Error loading manifest mapping: {e}")
        import traceback
        traceback.print_exc()
        return {}


def update_database_sources(manifest_mapping: dict, dry_run: bool = True):
    updated_count = 0
    not_found_count = 0
    already_correct_count = 0
    
    with app.app_context():
        from sqlalchemy.orm.attributes import flag_modified
        
        for entity_uri, manifest_url in manifest_mapping.items():
            identifier = entity_uri
            
            coll = Collection.query.filter_by(identifier=identifier).first()
            
            if not coll:
                print(f"⚠️  Collection not found: {identifier}")
                not_found_count += 1
                continue
            
            if not coll.dublin_core:
                coll.dublin_core = {}
            
            dublin_core = coll.dublin_core
            
            current_source = dublin_core.get('source', '')
            if current_source == manifest_url:
                print(f"✓ Already correct: {identifier}")
                already_correct_count += 1
                continue
            
            dublin_core['source'] = manifest_url
            
            if not dry_run:
                coll.dublin_core = dublin_core
                flag_modified(coll, "dublin_core")
                db.session.add(coll)
            
            print(f"{'[DRY RUN] ' if dry_run else ''}Updated {identifier}")
            print(f"  Old source: {current_source if current_source else '(empty)'}")
            print(f"  New source: {manifest_url}")
            print()
            
            updated_count += 1
        
        if not dry_run:
            db.session.commit()
            print(f"✅ Successfully updated {updated_count} collections")
        else:
            print(f"🔍 DRY RUN: Would update {updated_count} collections")
            print(f"   {already_correct_count} collections already have correct source")
            print(f"   {not_found_count} identifiers not found in database")
            print("\nRun with --apply to actually update the database")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_source_in_db.py <path_to_manifest_Mapping.json> [--apply]")
        print("\nExample:")
        print("  python update_source_in_db.py manifest_Mapping.json          # Dry run")
        print("  python update_source_in_db.py manifest_Mapping.json --apply  # Actually update")
        print("\nThe JSON file should contain mappings from entity URIs to manifest URLs:")
        print('  {')
        print('    "https://data.biblissima.fr/entity/Q243160": "https://api.irht.cnrs.fr/ark:/63955/fkrsdc504spf/manifest.json",')
        print('    "https://data.biblissima.fr/entity/Q206328": "https://api.irht.cnrs.fr/ark:/63955/fbq0iz8v3p55/manifest.json"')
        print('  }')
        sys.exit(1)
    
    mapping_path = sys.argv[1]
    apply = "--apply" in sys.argv
    
    if not os.path.exists(mapping_path):
        print(f"❌ Error: Manifest mapping file not found: {mapping_path}")
        sys.exit(1)
    
    manifest_mapping = load_manifest_mapping(mapping_path)
    
    if not manifest_mapping:
        print("❌ Error: No mappings loaded, exiting")
        sys.exit(1)
    
    update_database_sources(manifest_mapping, dry_run=not apply)
