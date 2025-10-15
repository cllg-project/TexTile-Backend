import csv
import itertools
import json
from typing import Literal, Dict, List, Any
from ..constants import basedir
import functools

SearchMode = Literal["exact", "fuzzy", "partial"]

VARIANTS = {}
with open(f"{basedir}/variants.csv") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        # print(row)
        if row["from"] not in VARIANTS:
            VARIANTS[row["from"]] = []
        VARIANTS[row["from"]].append(row["to"])


def generate_variants(
    token: str,
    max_variants: int = 2000
) -> List[str]:
    """
    Generate variants for `token`, supporting multi-character variant keys.
    - Always includes the original token.
    - Caps the total number of produced variants to `max_variants`.
    - Returns variants in a deterministic order with duplicates removed.
    """
    n = len(token)

    # pre-sort keys by length descending to find longer matches earlier (optional)
    keys = sorted(VARIANTS.keys(), key=len, reverse=True)

    @functools.lru_cache(maxsize=None)
    def helper(pos: int) -> List[str]:
        if pos >= n:
            return [""]  # single empty suffix

        out: List[str] = []

        # build choices at this position:
        # 1) keep the current single character
        choices = [(token[pos], pos + 1)]

        # 2) any variant key that matches token at this position
        for key in keys:
            if token.startswith(key, pos):
                for repl in VARIANTS[key]:
                    choices.append((repl, pos + len(key)))

        # for each choice, concatenate all suffixes
        for piece, next_pos in choices:
            for suffix in helper(next_pos):
                out.append(piece + suffix)
                if len(out) >= max_variants:
                    break
            if len(out) >= max_variants:
                break

        return out

    variants = helper(0)

    # Deduplicate while preserving order (original token will appear first)
    seen = set()
    deduped = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            deduped.append(v)
            if len(deduped) >= max_variants:
                break

    return deduped


def build_search_query(
        query: str,
        mode: SearchMode = "exact",
        include_variants: bool = False,
        variant_boost: float = 0.6,  # 👈 Lower boost for variants
        base_boost: float = 1.0  # 👈 Normal boost for exact user term
) -> Dict[str, Any]:
    """
    Build an Elasticsearch query body supporting character variation and differential boosting.
    """

    parts: List[str] = [p.strip() for p in query.split(",") if p.strip()]
    should_clauses: List[Dict[str, Any]] = []
    field = "content"

    for part in parts:
        # Quoted phrase
        if part.startswith('"') and part.endswith('"'):
            phrase = part.strip('"')
            clause = {"match_phrase": {field: {"query": phrase, "boost": base_boost}}}

        else:
            # Expand variants
            variants = generate_variants(part) if include_variants else [part]
            variants = [v for v in variants if v != part]  # exclude the main term itself

            variant_clauses = []

            # Base term (user-typed)
            if mode == "exact":
                if "*" in part:
                    variant_clauses.append({"wildcard": {field: {"value": part, "boost": base_boost}}})
                else:
                    variant_clauses.append({"match": {field: {"query": part, "operator": "and", "boost": base_boost}}})
            elif mode == "fuzzy":
                variant_clauses.append({
                    "bool": {
                        "should": [
                            {"match": {field: {"query": part, "operator": "and", "boost": base_boost * 5}}},
                            {"fuzzy": {
                                field: {"value": part, "fuzziness": 1, "prefix_length": 1, "boost": variant_boost*3}}},
                            {"fuzzy": {
                                field: {"value": part, "fuzziness": 2, "prefix_length": 2, "boost": variant_boost}}}
                        ],
                        "minimum_should_match": 1
                    }
                })
            elif mode == "partial":
                variant_clauses.append(
                    {
                        "bool": {
                            "should": [
                                {"match": {field: {"query": part, "operator": "and", "boost": base_boost * 5}}},
                                {"match": {
                                    f"{field}.ngram": {"query": part, "boost": base_boost}}}
                            ],
                            "minimum_should_match": 1
                        }
                    }
                )
                #    {"match": {f"{field}.ngram": {"query": part, #"analyzer": "ngram_analyzer",
                #         "boost": base_boost}}})
            else:
                raise ValueError(f"Unknown search mode: {mode}")

            # Add variants with lower boost
            for v in variants:
                if mode == "exact":
                    variant_clauses.append({"match": {field: {"query": v, "operator": "and", "boost": variant_boost}}})
                elif mode == "fuzzy":
                    variant_clauses.append({
                        "bool": {
                            "should": [
                                {"match": {field: {"query": v, "operator": "and", "boost": variant_boost * 3}}}
                            ],
                            "minimum_should_match": 1
                        }
                    })
                elif mode == "partial":
                    variant_clauses.append(
                        {"match": {field: {"query": v, "analyzer": "ngram_analyzer", "boost": variant_boost}}})

            clause = {"bool": {"should": variant_clauses, "minimum_should_match": 1}}

        should_clauses.append(clause)

    # Combine comma-separated queries with OR logic
    if len(should_clauses) == 1:
        query_body = {"query": should_clauses[0]}
    else:
        query_body = {"query": {"bool": {"should": should_clauses, "minimum_should_match": 1}}}

    return query_body

if __name__ == "__main__":
    import click
    @click.command("generate-query")
    @click.argument("query")
    @click.option("--password")
    @click.option("--variants", is_flag=True, default=False)
    @click.option("--mode", type=click.Choice(["exact", "fuzzy", "partial"]), default="exact")
    def generate(query, password, variants, mode):
        click.echo("Variant is deactivated")
        variants = False
        query = build_search_query(query, include_variants=variants, mode=mode)
        query["highlight"] = {
          "fields": {
            "content*": {"fragment_size": 100},
          },
          "pre_tags": ["<hit>"],
          "post_tags": ["</hit>"],
        }
        query["size"] = 100
        query["_source"] = False
        print(
            'docker exec -it $(docker ps --filter "name=dts_dts_elasticsearch" --format "{{.ID}}") '
            f'curl -u "elastic:{password}" '
            '-H "Content-Type: application/json" '
            '-X POST "http://localhost:9200/documents_v3/_search" '
            f"-d '{json.dumps(query)}'"
            " | jq -r '.hits.hits[].highlight'"
        )

    generate()