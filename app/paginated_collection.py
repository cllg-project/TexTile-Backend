import json
import os


from typing import Optional, Dict, List

import uritemplate

from flask import Response, request, url_for, Flask

from dapytains.app.app import msg_4xx, inject_json, get_templates
from dapytains.app.database import Collection, db


PAGINATION_SIZE = os.getenv("PAGINATION_SIZE", 20)


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

    # Apply sorting based on parameters
    if sort_by == "title" or sort_by == "alphabetical":
        if sort_order == "desc":
            query = query.order_by(Collection.title.desc())
        else:
            query = query.order_by(Collection.title)
    elif sort_by == "nb_children" or sort_by == "children":
        if sort_order == "desc":
            query = query.order_by(Collection.nb_children.desc())
        else:
            query = query.order_by(Collection.nb_children)
    else:  # default sorting
        if nav == 'children':
            # Default for children: nb_children first, then title
            if sort_order == "desc":
                query = query.order_by(Collection.nb_children.desc(), Collection.title.desc())
            else:
                query = query.order_by(Collection.nb_children, Collection.title)
        else:  # parents
            # Default for parents: title only
            if sort_order == "desc":
                query = query.order_by(Collection.title.desc())
            else:
                query = query.order_by(Collection.title)

    pagination = query.paginate(page=page, per_page=PAGINATION_SIZE, error_out=False)

    members = pagination.items
    last_page = pagination.pages if pagination.pages > 0 else 1

    def page_url(p):
        return url_for("collection_route", id=coll.identifier, nav=nav, page=p, sort_by=sort_by, sort_order=sort_order, _external=False)

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
        "@context": "https://distributed-text-services.github.io/specifications/context/1-alpha1.json",
        "dtsVersion": "1-alpha",
        **out,
        "member": [
            {
                **member.json(inject=inject_json(member, templates=templates)),
                "nb_children": getattr(member, 'nb_children', None)
            }
            for member in members
        ],
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
