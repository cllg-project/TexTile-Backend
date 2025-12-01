import json
import glob
import importlib
import os.path
from functools import partial
from pathlib import Path
from typing import Callable, Tuple, Dict, Set, Union, List, Any
from multiprocessing import Pool, cpu_count

import lxml.etree as et
import click
import tqdm


def set_nested(result: Dict[str, Any], path: str, value: Any) -> None:
    """
    Assign value to a nested dictionary path like "a/b/c".
    If the key already exists, convert to list or append.
    """
    keys = path.split("/")
    d = result

    for key in keys[:-1]:
        if key not in d or not isinstance(d[key], dict):
            d[key] = {}
        d = d[key]

    last = keys[-1]

    # Value collision → turn into list
    if last in d:
        if isinstance(d[last], list) and value not in d[last]:
            d[last].append(value)
        elif isinstance(d[last], str) and d[last] != value:
            d[last] = [d[last], value]
    else:
        d[last] = value


def extract_text(node: Union[et._Element, et._ElementUnicodeResult]) -> str:
    """Extract text from element or attribute node."""
    if isinstance(node, et._Element):
        return (node.text or "").strip()
    return str(node).strip()

def extract_dublin_core(
    mapping: Dict[str, Union[str, Dict[str, Any]]],
    node: et._Element
) -> Dict[str, Any]:
    """
    Given:
      - mapping: dict {xpath: "path/to/key"} or {xpath: {"target": "...", "split": "..."}}
      - node: an lxml element

    Returns a nested dictionary based on target paths.
    """
    result: Dict[str, Any] = {}
    ns = dict(namespaces={"tei": "http://www.tei-c.org/ns/1.0"})

    for xpath, target in mapping:
        # Handle dict target with split
        if isinstance(target, dict):
            target_path = target["target"]
            split_token = target.get("split")
            replace = target.get("replace", [])
        else:
            target_path = target
            split_token = None
            replace = []

        matches = node.xpath(xpath, **ns)

        if not matches:
            continue

        extracted_values: List[Any] = []

        for m in matches:
            text = extract_text(m)
            if not text:
                continue

            outs = []
            if split_token:
                parts = [p for p in text.split(split_token) if p]
                outs.extend(parts)
            else:
                outs.append(text)

            if replace:
                for value in outs:
                    for src, tgt in replace:
                        value = value.replace(src, tgt)
                    extracted_values.append(value)
            else:
                extracted_values.extend(outs)

        # Assign the extracted values
        for val in extracted_values:
            set_nested(result, target_path, val)

    return result

def load_function_from_path(path: str, func_name: str = "parse_metadata") -> Callable[
    [str, Dict[str, str]], Tuple[Dict[str, Dict], Set[Tuple[str, str]]]
]:
    """Dynamically load a function from a Python file."""
    file_path = Path(path).resolve()

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    spec = importlib.util.spec_from_file_location("dynamic_module", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, func_name):
        raise AttributeError(f"{func_name} not found in {file_path}")

    func = getattr(module, func_name)
    if not callable(func):
        raise TypeError(f"{func_name} in {file_path} is not callable")

    return func


def parse_metadata(
        filepath: str,
        mapping: Dict[str, str],
        additional_metadata: Dict[str, Dict[str, str]] = None
) -> Tuple[Dict[str, Dict], Set[Tuple[str, str]]]:
    """This function parses TEI XML files and generates the catalog structure from it.
    It uses @xml:id="root_collection", @prev, and @xml:id="leaf_resource" to build the
    relationships of items (the structure). It reuses the ./tei:name/text() value
    and ./tei:idno/text() to infer collection's id and names.
    """
    if not additional_metadata:
        additional_metadata = {}

    ns = dict(namespaces={"tei": "http://www.tei-c.org/ns/1.0"})
    # This is the basic metadata parsing script
    xml = et.parse(filepath)

    collections: Dict[str, Dict] = {}
    links: Set[Tuple[str, str]] = set()

    def name_and_uri(node: et._Element) -> Tuple[str, str]:
        coll_name = [
            str(name.text)
            for name in node.xpath("./tei:name", **ns)
        ][0]
        identifier = [
            str(idno.text)
            for idno in node.xpath("./tei:idno", **ns)
        ][0]
        return coll_name, identifier

    # Find the leaf, then get up
    node = xml.xpath(".//*[@xml:id='leaf_resource']")[0]
    name, last_uri = name_and_uri(node)
    collections[last_uri] = {"title": name, "filepath": filepath}
    collections[last_uri].update(extract_dublin_core(mapping, xml))
    for jpath, values in additional_metadata.get(last_uri, {}).items():
        set_nested(collections[last_uri], jpath, values)

    while node.attrib.get("prev"):
        node = xml.xpath(f".//*[@xml:id='{node.attrib['prev'][1:]}']")[0]
        name, current_uri = name_and_uri(node)
        collections[current_uri] = {"title": name}
        links.add((current_uri, last_uri))
        last_uri = current_uri
        for jpath, values in additional_metadata.get(last_uri, {}).items():
            set_nested(collections[last_uri], jpath, values)

    return collections, links

def dict_to_xml(parent, data):
    """
    Recursively converts a dictionary (possibly nested) into XML nodes
    under the given parent element, using lxml.etree.SubElement.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                # create parent node for nested structures
                elem = et.SubElement(parent, key)
                dict_to_xml(elem, value)
            else:
                # primitive value → single element with text
                elem = et.SubElement(parent, key)
                elem.text = str(value)

    elif isinstance(data, list):
        for item in data:
            # repeated tag uses the current parent tag name
            if isinstance(item, (dict, list)):
                elem = et.SubElement(parent, parent.tag)
                dict_to_xml(elem, item)
            else:
                elem = et.SubElement(parent, parent.tag)
                elem.text = str(item)

    else:
        parent.text = str(data)


def catalog_to_xml(data: Dict[str, Dict]) -> str:
    root = et.Element("collection", {"identifier": "mega-corpus"})
    (et.SubElement(root, "title")).text = "Root Collection"

    members_root = et.SubElement(root, "members")

    def treat_level(level: Dict[str, Any], parent: et._Element):
        kind = "resource" if level.get("filepath") else "collection"
        node = et.SubElement(parent, kind, attrib={"identifier": level["id"]})
        if kind == "resource":
            node.attrib["filepath"] = os.path.abspath(level["filepath"])

        # Add Metadata
        dict_to_xml(
            node,
            {
                key: value
                for key, value in level.items()
                if key not in {"members", "id", "filepath"}
            }
        )
        members_level = et.SubElement(node, "members")

        for level in level.get("members", []):
            treat_level(level, members_level)

    for level in data.values():
        treat_level(level, members_root)

    return et.tostring(root, encoding=str)


def build_catalog_dict(collections: Dict[str, Dict], links: Set[Tuple[str, str]]) -> Dict[str, Dict]:
    """ Build the catalog dictionary before transforming to XML
    """
    organized_collections = {}
    link_dicts = {}
    for s, t in links:
        link_dicts.setdefault(s, []).append(t)

    def augment(collection_id: str):
        return {
            "id": collection_id,
            **collections[collection_id],
            "members": sorted(
                [augment(member) for member in link_dicts.get(collection_id, [])],
                key=lambda x: x["title"]
            )
        }

    # Find root collections
    sources, targets = zip(*list(links))
    level = set([source for source in set(sources) if source not in targets])
    for element in level:
        organized_collections[element] = {
            "id": element,
            **collections[element],
            "members": sorted(
                [augment(member) for member in link_dicts[element]],
                key=lambda x: x["title"]
            )
        }
    return organized_collections


@click.command()
@click.argument("wildcard_path", type=str)
@click.option("-o", "--output", default="catalog.xml",
              type=click.Path(file_okay=True, dir_okay=False)
              )
@click.option("-w", "--workers", default=cpu_count() - 1,
              type=int, help="Number of workers to process the corpus"
              )
@click.option("--external-function",
              type=click.Path(file_okay=True, dir_okay=False),
              help="Path to a python script with a function `parse_metadata` (See "
                   "documentation from the profile function)")
@click.option("--external-metadata",
              type=click.File(mode="r"),
              help="Path to a JSON file with metadata, such that key -> Dict"
                   " where dict has keys using `/` syntax and values are what you "
                   "want to input.")
@click.option("--mapping", type=click.File(mode="r"),
              help="A JSON file with mappings available for the parsing function")
def build(wildcard_path, output: str, workers: int, external_function,
          external_metadata, mapping):
    """ Builds a dynamic catalog.xml file using metadata from the XML files themselves

    :param wildcard_path:
    :param output:
    :param workers:
    :param external_function:
    :return:
    """

    files = glob.glob(wildcard_path, recursive=True)
    if external_function:
        external_function = load_function_from_path(
            external_function,
            "parse_metadata"
        )
    else:
        external_function = parse_metadata

    collections = {}
    links = set()

    if mapping:
        mapping = json.load(mapping)
    else:
        mapping = []

    if external_metadata:
        external_metadata = json.load(external_metadata)

    external_function = partial(external_function, mapping=mapping, additional_metadata=external_metadata)

    pbar = tqdm.tqdm()
    with Pool(processes=workers) as pool:
        for parse_collections, parsed_links in pool.imap_unordered(external_function, files):
            collections.update(parse_collections)
            links = links.union(parsed_links)
            pbar.update(1)

    catalog_obj = build_catalog_dict(collections, links)
    catalog_xml = catalog_to_xml(catalog_obj)
    with open(output, "w") as f:
        f.write(catalog_xml)

if __name__ == "__main__":


    build()
