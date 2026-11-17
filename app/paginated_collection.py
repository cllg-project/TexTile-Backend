import json
import os


from typing import Optional, Dict, List

import uritemplate

from flask import Response, request, url_for, Flask

import dapytains.app.app as dts_app
from dapytains.app.app import CONTEXT_URL, DTS_VERSION, msg_4xx, inject_json
from dapytains.app.database import Collection, db


PAGINATION_SIZE = int(os.getenv("PAGINATION_SIZE", "20"))

# The library's own implementation, captured before install_templates() replaces it
_library_get_templates = dts_app.get_templates


def get_templates(base_uri: str):
    """
    DTS URI templates, with the `page` parameter on the collection endpoint since this API
    paginates collections (the DTS 1.0 specification lists it: `{?id,page,nav}`). dapytains
    itself only advertises `{?id}{&nav}`.

    Written `{?id}{&page,nav}`, which is the same template (RFC 6570) but keeps the remaining
    variables when a link is partially expanded for one collection (`...?id=x{&page,nav}`).
    """
    _, document_template, navigation_template = _library_get_templates(base_uri)
    collection_template = uritemplate.URITemplate(base_uri + "collection/{?id}{&page,nav}")
    return collection_template, document_template, navigation_template


def install_templates():
    """
    Make dapytains' own routes (entry point, navigation) use get_templates() above too.
    They look the function up in their module at call time, so replacing it is enough.
    """
    dts_app.get_templates = get_templates


def _member_json(member: Collection, templates) -> Dict:
    """A DTS member. Our own `nb_children` goes in `extensions`, where the DTS spec puts custom properties."""
    data = member.json(inject=inject_json(member, templates=templates))
    count = getattr(member, "nb_children", None)
    if count is not None:
        data["extensions"] = {**data.get("extensions", {}), "nb_children": [count]}
    return data


def collection_view(
        identifier: Optional[str],
        nav: str,
        page: int,
        templates: Dict[str, uritemplate.URITemplate],
        media_types: List[str] = None,
        sort_by: str = "default",
        sort_order: str = "asc"
) -> Response:
    """ Builds a collection view, regardless of how the parameters are received

    :param identifier:
    :param nav:
    :param templates:
    """
    if not identifier:
        coll: Collection = db.session.query(Collection).filter(~Collection.parents.any()).first()
    else:
        coll = Collection.query.where(Collection.identifier==identifier).first()
    if coll is None:
        return msg_4xx("Unknown collection")
    out = coll.json()

    if coll.resource:
        if media_types:
            out["mediaTypes"] = media_types

    if nav == 'children':
        query = db.session.query(Collection).filter(
            Collection.parents.any(id=coll.id)
        )
    elif nav == 'parents':
        query = db.session.query(Collection).filter(
            Collection.children.any(id=coll.id)
        )
    else:
        return msg_4xx(f"nav parameter has a wrong value {nav}", code=400)

    # Sorting. Ties are always broken alphabetically (ascending), whatever the direction asked for.
    title_order = Collection.title.desc() if sort_order == "desc" else Collection.title
    if sort_by in ("title", "alphabetical"):
        query = query.order_by(title_order)
    elif sort_by in ("nb_children", "children"):
        size_order = Collection.nb_children.desc() if sort_order == "desc" else Collection.nb_children
        query = query.order_by(size_order, Collection.title)
    elif nav == "children":
        # Default for children: largest first, then alphabetical
        query = query.order_by(Collection.nb_children.desc(), Collection.title)
    else:
        # Default for parents: title only
        query = query.order_by(title_order)

    pagination = query.paginate(page=page, per_page=PAGINATION_SIZE, error_out=False)

    members = pagination.items
    last_page = pagination.pages if pagination.pages > 0 else 1
    if page > last_page:
        return msg_4xx(f"Page {page} does not exist, the last page is {last_page}")

    def page_url(p):
        return url_for("collection_route", id=coll.identifier, nav=nav, page=p, sort_by=sort_by, sort_order=sort_order, _external=True)

    view = {
        "@id": page_url(page),
        "@type": "Pagination",
        "first": page_url(1),
        "last": page_url(last_page)
    }
    if pagination.has_prev:
        view["previous"] = page_url(pagination.prev_num)
    if pagination.has_next:
        view["next"] = page_url(pagination.next_num)

    return Response(json.dumps({
        "@context": CONTEXT_URL,
        "dtsVersion": DTS_VERSION,
        **out,
        "member": [_member_json(member, templates) for member in members],
        **inject_json(coll, templates=templates),
        "view": view
    }, ), mimetype="application/ld+json", status=200)



def change_collection_route(app: Flask, media_types: List[str]):
    app.view_functions.pop('collection_route')

    @app.route("/collection/")
    def collection_route():
        resource = request.args.get("id")
        nav = request.args.get("nav", "children")
        page = request.args.get("page", 1, type=int)
        sort_by = request.args.get("sort_by", "default")
        sort_order = request.args.get("sort_order", "asc")
        collection_template, document_template, navigation_template = get_templates(request.url_root)

        return collection_view(resource, nav, page=page, sort_by=sort_by, sort_order=sort_order, templates={
            "navigation": navigation_template,
            "collection": collection_template,
            "document": document_template,
        }, media_types=media_types)
