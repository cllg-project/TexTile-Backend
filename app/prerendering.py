"""
Prerendering module with disk and database caching.

Environment variables:
- USE_DISK_CACHE: Enable/disable disk caching (default: "true")
- PRERENDER_DIR: Directory for disk cache (default: "/tmp/prerender")

To migrate from database to disk cache:
    flask data temp-db-to-dir

To check cache statistics:
    flask data cache-stats

To clear disk cache:
    flask data clear-disk-cache --confirm
"""

from typing import Optional, Dict

import hashlib
import json
import logging
import os
from pathlib import Path
import saxonche
from dapytains.app.app import msg_4xx
from dapytains.app.database import db, Collection, Navigation
from dapytains.app.transformer import Transformer
from dapytains.processor import get_processor
from dapytains.tei.document import Document
from flask import Response, Flask, request
from lxml import etree as ET

# Set up logger for cache operations
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DEFAULT_MEDIA_TYPE = "application/xml"


DEFAULT_MEDIA_TYPE = "application/xml"


class DiskPrerenderedCacheClass:
    """Disk-based cache for prerendered pages using SHA-based subfolders."""

    def __init__(self, verbose: bool = False, root: Optional[Path] = None, levels: int = 3,
                 chars_per_level: int = 2):
        self.verbose = verbose
        self.ROOT = root or Path(os.getenv("PRERENDER_DIR", "/tmp/prerender"))
        self.LEVELS = levels
        self.CHARS_PER_LEVEL = chars_per_level
        os.makedirs(str(self.ROOT), exist_ok=True)

    @staticmethod
    def short_sha(s: str, length: int = 8) -> str:
        """Return a short SHA hash of a string."""
        return hashlib.sha1(s.encode("utf-8")).hexdigest()[:length]

    @staticmethod
    def safe_filename(s: str) -> str:
        """Make a string safe for a filename."""
        return DiskPrerenderedCacheClass.short_sha(s)

    def sha_subfolders(self, identifier: str) -> str:
        """Generate nested subfolder path from SHA of identifier."""
        sha = hashlib.sha1(identifier.encode("utf-8")).hexdigest()
        parts = [sha[i * self.CHARS_PER_LEVEL:(i + 1) * self.CHARS_PER_LEVEL]
                 for i in range(self.LEVELS)]
        return os.path.join(*parts, sha)

    def get_cache_path(self, identifier: str, ref: str, end: Optional[str], media: str, tree: str) -> Path:
        """Return the full path for a prerendered page based on inputs."""
        base = self.ROOT / self.sha_subfolders(identifier)
        filename_parts = [self.safe_filename(tree), self.safe_filename(ref)]
        if end:
            filename_parts.append(self.safe_filename(end))
        filename_parts.append(self.safe_filename(media))
        filename = "__".join(filename_parts) + ".prerender"
        return base / filename

    def get_cache(self, collection: Collection, ref: str, end: Optional[str], media: str, tree: str) -> Optional[str]:
        """Retrieve cached content if it exists."""
        path = self.get_cache_path(collection.identifier, ref, end, media, tree)
        if not path.exists():
            if self.verbose:
                print(f"💾 DISK CACHE MISS: {collection.identifier} ?ref={ref} ({media}) {path}")
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            content = data.get("content")
            if content:
                if self.verbose:
                    print(f"✅ DISK CACHE HIT: {collection.identifier} ?ref={ref} ({media}) - {len(content)} chars")
            return content
        except (json.JSONDecodeError, IOError):
            if self.verbose:
                print(f"❌ DISK CACHE ERROR: {collection.identifier}/{ref} ({media}) - file corrupted")
            return None

    def save_cache(self, identifier: str, ref: str, end: Optional[str], media: str, tree: str, content: str) -> None:
        """Save content to cache."""
        path = self.get_cache_path(identifier, ref, end, media, tree)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"content": content}, f)
            if self.verbose:
                print(f"💾 SAVED TO DISK CACHE: {identifier}/{ref} ({media}) - {len(content)} chars")
        except IOError as e:
            if self.verbose:
                print(f"❌ DISK CACHE SAVE ERROR: {identifier}/{ref} ({media}) - {e}")
            pass  # Silently fail if we can't write to disk


DiskPrerenderedCache = DiskPrerenderedCacheClass(verbose=bool(int(os.getenv("VERBOSE_CACHE", "0"))))


class CustomXSLTransformer(Transformer):
    def __init__(self, xslts: Dict[str, str], media_type_mapping: Dict[str, str] = None):
        super().__init__(supported_media_types=set(xslts.keys()))
        self.xslts = xslts
        self.mapping: Dict[str, str] = media_type_mapping or {}

    def transform(self, media: str, collection: Collection, document: ET.ElementTree) -> Response:
        if media not in self.supported_media_types:
            return super().transform(media, collection, document)

        with get_processor() as processor:
            xslt_proc = processor.new_xslt30_processor()
            xslt_proc.set_cwd(".")
            transformer = xslt_proc.compile_stylesheet(stylesheet_file=self.xslts[media])
            document_builder = processor.new_document_builder()
            if not isinstance(document, str):
                document = ET.tostring(document, encoding=str)
            transformed = transformer.transform_to_string(
                xdm_node=document_builder.parse_xml(xml_text=document)
            )
        del processor  # This keeps memory clean
        # processor.detach_current_thread
        return Response(
            transformed,
            status=200,
            mimetype=self.mapping.get(media, media)
        )


def get_xml_passage_as_string(collection: Collection, ref: str, end: str, tree: str) -> str:
    with saxonche.PySaxonProcessor() as processor:
        doc = Document(collection.filepath, processor=processor)
        passage: str = ET.tostring(
            doc.get_passage(ref_or_start=ref, end=end, tree=tree), encoding=str
        )
    return passage

def get_transformed(media: str, collection: Collection, passage: str, transformer: Transformer) -> Response:
    return transformer.transform(media, collection, ET.fromstring(passage))

def get_xml_passage_or_cache(collection: Collection, ref: str, end: str, tree) -> str:
    # This function avoids repeating stuff.
    use_disk_cache = os.getenv("USE_DISK_CACHE", "true").lower() == "true"
    
    if use_disk_cache:
        # First try disk cache
        cached_content = DiskPrerenderedCache.get_cache(collection=collection, ref=ref, end=end, media="application/xml", tree=tree)
        if cached_content:
            return cached_content

    logger.info(f"🔄 GENERATING CONTENT: {collection.identifier}/{ref} (application/xml)")
    passage: str = get_xml_passage_as_string(collection, ref, end, tree)
    
    return passage


# This view should have some kind of REDIS cache, specifically for pages with high hits ?
def custom_document_view(resource, ref, start, end, tree, media, transformer: Transformer) -> Response:
    if not resource:
        return msg_4xx("Resource parameter was not provided")

    collection: Collection = Collection.query.where(Collection.identifier == resource).first()
    if not collection:
        return msg_4xx(f"Unknown resource `{resource}`")

    nav: Navigation = Navigation.query.where(Navigation.collection_id == collection.id).first()
    if nav is None:
        return msg_4xx(f"The resource `{resource}` does not support navigation")

    tree = tree or collection.default_tree

    # Check for forbidden combinations
    if ref or start or end:
        if tree not in nav.references:
            return msg_4xx(f"Unknown tree {tree} for resource `{resource}`")
        elif ref and (start or end):
            return msg_4xx(f"You cannot provide a ref parameter as well as start or end", code=400)
        elif not ref and ((start and not end) or (end and not start)):
            return msg_4xx(f"Range is missing one of its parameters (start or end)", code=400)

    paths = nav.paths[tree]
    if start and end and (start not in paths or end not in paths):
        return msg_4xx(f"Unknown reference {start} or {end} in the requested tree.", code=404)
    if ref and ref not in paths:
        return msg_4xx(f"Unknown reference {ref} in the requested tree.", code=404)

    if not ref and not start:
        with open(collection.filepath) as f:
            content = f.read()
        return Response(content, mimetype=DEFAULT_MEDIA_TYPE)

    media = media or DEFAULT_MEDIA_TYPE

    if media != DEFAULT_MEDIA_TYPE:
        use_disk_cache = os.getenv("USE_DISK_CACHE", "true").lower() == "true"
        
        if use_disk_cache:
            # First try disk cache for the transformed content
            cached_content = DiskPrerenderedCache.get_cache(collection=collection, ref=ref or start, end=end, media=media, tree=tree)
            if cached_content:
                return Response(cached_content, status=200, mimetype=transformer.mapping.get(media, media))
        
        # Otherwise, get the base XML passage and transform it
        passage: str = get_xml_passage_or_cache(collection, ref or start, end, tree)
        logger.info(f"🔄 TRANSFORMING CONTENT: {collection.identifier}/{ref or start}/{end}/{tree} ({media})")
        response: Response = get_transformed(
            media=media,
            collection=collection,
            transformer=transformer,
            passage=passage
        )
        
        # Save the transformed content to disk cache (if enabled)
        if use_disk_cache:
            DiskPrerenderedCache.save_cache(collection.identifier, ref or start, end, media, tree, response.get_data(as_text=True))
        
        return response
    else:
        passage: str = get_xml_passage_or_cache(collection, ref or start, end, tree)
        return Response(passage, status=200, mimetype="application/xml")



def change_document_route(app: Flask, media_transformer: Transformer):
    app.view_functions.pop('document_route')

    @app.route("/document/")
    def document_route():
        resource = request.args.get("resource")
        ref = request.args.get("ref")
        start = request.args.get("start")
        end = request.args.get("end")
        tree = request.args.get("tree")
        media = request.args.get("mediaType")
        return custom_document_view(resource, ref, start, end, tree, media=media, transformer=media_transformer)
