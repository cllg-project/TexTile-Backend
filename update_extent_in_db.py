#!/usr/bin/env python
import os
import re
import xml.etree.ElementTree as ET
from flask import Flask
from app import app, db
from dapytains.app.database import Collection

def parse_extent_from_catalog(catalog_path: str) -> dict:
    extent_map = {}
    
    try:
        tree = ET.parse(catalog_path)
        root = tree.getroot()
        
        for resource in root.findall(".//resource"):
            identifier = resource.get('identifier')
            if not identifier:
                continue
            
            dublin_core = resource.find("dublinCore")
            if dublin_core is None:
                continue
            
            extent_elem = dublin_core.find("extent")
            if extent_elem is not None:
                extent_texts = []
                tokens = None
                ocr_quality = None
                
                for nested_extent in extent_elem.findall("extent"):
                    if nested_extent.text:
                        extent_texts.append(nested_extent.text)
                        
                        tokens_match = re.search(r'Tokens:\s*(\d+)', nested_extent.text)
                        if tokens_match:
                            tokens = int(tokens_match.group(1))
                        
                        if 'Acceptable OCRed lines' in nested_extent.text:
                            ocr_quality = nested_extent.text
                
                if extent_texts:
                    extent_map[identifier] = {
                        'texts': extent_texts,
                        'tokens': tokens,
                        'ocr_quality': ocr_quality
                    }
    
    except Exception as e:
        print(f"Error parsing catalog: {e}")
        import traceback
        traceback.print_exc()
    
    return extent_map


def update_database_extents(catalog_path: str, dry_run: bool = True):
    print(f"Parsing catalog.xml from: {catalog_path}")
    extent_map = parse_extent_from_catalog(catalog_path)
    print(f"Found extent data for {len(extent_map)} collections")
    
    updated_count = 0
    
    with app.app_context():
        from sqlalchemy.orm.attributes import flag_modified
        
        collections = Collection.query.all()
        
        for coll in collections:
            if coll.identifier in extent_map:
                extent_data = extent_map[coll.identifier]
                
                if coll.dublin_core:
                    dublin_core = coll.dublin_core
                    
                    dublin_core['extent'] = extent_data['texts']
                    
                    if not dry_run:
                        coll.dublin_core = dublin_core
                        flag_modified(coll, "dublin_core")
                        db.session.add(coll)
                    
                    print(f"{'[DRY RUN] ' if dry_run else ''}Updated {coll.identifier}")
                    print(f"  Extent: {extent_data['texts']}")
                    print(f"  Tokens: {extent_data['tokens']}")
                    print(f"  OCR Quality: {extent_data['ocr_quality']}")
                    print()
                    
                    updated_count += 1
        
        if not dry_run:
            db.session.commit()
            print(f"✅ Successfully updated {updated_count} collections")
        else:
            print(f"🔍 DRY RUN: Would update {updated_count} collections")
            print("Run with --apply to actually update the database")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python update_extent_in_db.py <path_to_catalog.xml> [--apply]")
        print("\nExample:")
        print("  python update_extent_in_db.py /path/to/catalog.xml          # Dry run")
        print("  python update_extent_in_db.py /path/to/catalog.xml --apply  # Actually update")
        sys.exit(1)
    
    catalog_path = sys.argv[1]
    apply = "--apply" in sys.argv
    
    if not os.path.exists(catalog_path):
        print(f"❌ Error: Catalog file not found: {catalog_path}")
        sys.exit(1)
    
    update_database_extents(catalog_path, dry_run=not apply)
