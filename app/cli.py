import json
import multiprocessing
import os

from typing import List, Dict, Tuple

import click

from flask import Flask, current_app
from bs4 import BeautifulSoup

from dapytains.app.database import Collection

import warnings
from bs4 import XMLParsedAsHTMLWarning

# Suppress BeautifulSoup XML warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from dapytains.app.ingest import store_catalog
from dapytains.metadata.xml_parser import parse
from dapytains.app.database import db

from .transformation import media_transformer
from .constants import basedir


@click.group("db")
def db_group():
    """ Group for DB command """
    return

@db_group.command("create")
def db_create():
    """Create the db"""
    with current_app.app_context():
        db.create_all()

@db_group.command("reset")
def db_reset():
    """Drop and create the db"""
    with current_app.app_context():
        db.drop_all()
        db.create_all()


@db_group.command("count-children")
def count_children():
    """ Populate nb_children """
    with current_app.app_context():
        for coll in Collection.query.all():
            coll.nb_children = coll.total_children
        db.session.commit()


@db_group.command("count-manuscripts")
def count_manuscripts():
    """ Count total number of manuscripts (resources) in the database """
    with current_app.app_context():
        total = Collection.query.filter(Collection.resource == True).count()
        print(f"Total manuscripts in database: {total}")
        return total


@click.group("data")
def data_group():
    """Data related commands """
    return


@data_group.command("catalog-ingest")
@click.argument("catalog_filepath", type=click.Path(file_okay=True, dir_okay=False, readable=True))
def ingest(catalog_filepath):
    """Ingest the catalog file to store in the database """
    with current_app.app_context():
        catalog, _ = parse(catalog_filepath)
        store_catalog(catalog)


def _prerender_collection(params: Tuple[int, List[str], bool]) -> int:
    """
    Prerenders passages for a given navigation ID and set of media types.

    Args:
        params: A tuple containing:
            - navigation_id (int): The navigation ID.
            - media_types (List[str]): The list of media types to generate.
            - force_generate (bool): Whether to only create new stuff (False) or overwrite stuff (True)

    Returns:
        List[Dict]: A list of dictionaries with prerendered passage data
        that can be stored in db.Cache.

    ToDo: At some point, it would be great
    """
    from dapytains.app.database import Navigation
    from .prerendering import DEFAULT_MEDIA_TYPE, get_xml_passage_as_string, get_transformed, DiskPrerenderedCache

    navigation_id, media_types, force_generate = params
    rendered_content: int = 0

    with current_app.app_context():
        # Retrieve navigation and collection objects
        navigation = Navigation.query.get(navigation_id)
        collection = Collection.query.get(navigation.collection_id)

        # Iterate over references in navigation
        for tree, references in navigation.references.items():
            for ref in references:  # Assuming 1-depth structure
                ref_id = ref["identifier"]
                base_content = DiskPrerenderedCache.get_cache(
                    collection=collection, ref=ref_id, end=None,
                    media=DEFAULT_MEDIA_TYPE, tree=tree
                )
                # Base passage (default media type, untransformed XML)
                if not base_content or force_generate is True: # If we don't have prerender already and we don't force it
                    base_content = get_xml_passage_as_string(
                        collection=collection, ref=ref_id, end=None, tree=tree
                    )
                    DiskPrerenderedCache.save_cache(
                        identifier=collection.identifier, ref=ref_id, end=None,
                        media=DEFAULT_MEDIA_TYPE, tree=tree, content=base_content
                    )
                    rendered_content += 1

                # Additional transformed passages for each media type
                for media_type in media_types:
                    if DiskPrerenderedCache.get_cache(
                        collection=collection, ref=ref_id, end=None,
                        media=media_type, tree=tree
                    ) is None or force_generate is True:
                        transformed_content: str = get_transformed(
                            collection=collection,
                            media=media_type,
                            transformer=media_transformer,
                            passage=base_content,
                        ).get_data().decode()
                        DiskPrerenderedCache.save_cache(
                            identifier=collection.identifier, ref=ref_id, end=None,
                            media=media_type, tree=tree, content=transformed_content
                        )
                        rendered_content += 1
    return rendered_content

@data_group.group("prerender")
def prerender_group():
    """Prerender the pages """
    return

@prerender_group.command("generate")
@click.option("--media-type", default=None, multiple=True, type=str, help="mediaType to generate the cache for")
@click.option("--workers", default=1, type=int, help="Number of workers")
@click.option("--force", is_flag=True, default=False, help="Force regeneration of cache")
def prerender(media_type: List[str], workers: int, force: bool):
    """ Builds the prerendering for each CitableUnit found in each Navigation answer for each collection,
    potentially for a list of media types

    Example : flask data prerender --workers 2 --media-type=html
    """
    from dapytains.app.database import Navigation
    import tqdm

    with current_app.app_context():
        navigations = [navigation.id for navigation in Navigation.query.all()]

    # Use all available CPUs or limit if desired
    num_workers = min(workers, len(navigations))

    pbar = tqdm.tqdm(len(navigations))

    mss = 0
    pages = 0
    with multiprocessing.Pool(processes=num_workers) as pool:
        for generated in pool.imap_unordered(_prerender_collection, [(nav_id, media_type, force) for nav_id in navigations]):
            pages += generated
            mss += 1
            pbar.update(1)
            pbar.set_description(f"Manuscripts done: {mss}. Pages cached: {pages} ({pages/pbar.format_dict['elapsed']:.2f} p/s).")


def process_cache(navigation_id: int) -> List[Dict]:
    # Each process creates its own session
    from .prerendering import Navigation, DiskPrerenderedCache
    # Fast text extraction using regex (avoid BeautifulSoup overhead)
    import re

    returned_values = []
    with current_app.app_context():
        # Retrieve navigation and collection objects
        navigation = Navigation.query.get(navigation_id)
        collection = Collection.query.get(navigation.collection_id)

        # Iterate over references in navigation
        for tree, references in navigation.references.items():
            for ref in references:  # Assuming 1-depth structure
                prerender = DiskPrerenderedCache.get_cache(
                    collection=collection, ref=ref["identifier"], end=None,
                    media="html", tree=tree
                )
                content_match = re.search(r'<div id="rendered-tei"[^>]*>(.*?)</div>', prerender, re.DOTALL)
                if content_match:
                    content_html = content_match.group(1)
                    content_text = re.sub(r'<[^>]+>', '', content_html)  # Strip HTML tags
                    content_text = re.sub(r'\s+', ' ', content_text).strip()  # Normalize whitespace
                else:
                    content_text = ""

                returned_values.append({
                    "collection": collection.identifier,  # Use cache.identifier directly (already available)
                    "ref": ref["identifier"],
                    "content": content_text
                })
    return returned_values


@prerender_group.command("clear")
@click.option("--confirm", is_flag=True, help="Confirm deletion of disk cache")
def clear_disk_cache(confirm):
    """Clear the disk cache directory."""
    from .prerendering import DiskPrerenderedCache
    import shutil
    
    cache_dir = DiskPrerenderedCache.ROOT
    if not cache_dir.exists():
        click.echo(f"Cache directory {cache_dir} does not exist.")
        return
    
    if not confirm:
        click.confirm(
            f"This will delete ALL cached files in {cache_dir}. Are you sure?",
            abort=True
        )
    
    try:
        shutil.rmtree(cache_dir)
        click.echo(f"Disk cache cleared: {cache_dir}")
    except Exception as e:
        click.echo(f"Error clearing cache: {e}")

@data_group.command("prerender-to-json")
@click.option("-o", "--out", type=click.Path(file_okay=True, dir_okay=False), default="refs.json",
              help="Path where to save the json")
@click.option("--workers", type=int, default=2, help="Number of workers to use")
def generate_json(out, workers=2):
    """Generate a json of {identifier, ref, text} for ElasticSearch ingestion"""
    import tqdm
    from dapytains.app.database import Navigation
    data = []

    with current_app.app_context():
        navigations = [navigation.id for navigation in Navigation.query.all()]
        pbar = tqdm.tqdm(desc="Converted pages", total=len(navigations))
        with multiprocessing.Pool(processes=workers) as pool:
            for result in pool.imap_unordered(process_cache, navigations):
                pbar.update(1)
                if result:
                    data.append(result)
    with open(out, "w") as f:
        json.dump(data, f)

def register_cli(app: Flask):
    app.cli.add_command(db_group)
    app.cli.add_command(data_group)


