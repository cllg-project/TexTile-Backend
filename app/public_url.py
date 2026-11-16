"""
Make the API generate links for the public address of the website.

dapytains (and our own routes) build every link from the incoming request
(`request.url_root`, `url_for`). Behind a reverse proxy that is the internal address, e.g.
`http://localhost:5000/`, not the one users reach. Two settings fix that:

PUBLIC_BASE_URL
    The public address of the API, e.g. `https://comma.example.org/` or
    `https://example.org/dts/` (with a path prefix). Links are generated from it,
    whatever address the request arrived on. A proxy may strip the prefix or keep it.

TRUSTED_PROXY_HOPS
    Alternative when the proxy sends `X-Forwarded-Proto/Host/Prefix`: the number of
    proxies to trust (usually 1). Ignored when PUBLIC_BASE_URL is set, because trusting
    forwarded headers lets any client rewrite the links.

With neither set, links follow the request as before (fine for local development).
"""
import os
from urllib.parse import urlsplit

from werkzeug.middleware.proxy_fix import ProxyFix


class PublicURLMiddleware:
    """WSGI middleware pinning scheme, host and path prefix to a configured public URL."""

    def __init__(self, wsgi_app, public_url: str):
        parts = urlsplit(public_url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError(f"PUBLIC_BASE_URL must be an absolute http(s) URL, got {public_url!r}")
        self.wsgi_app = wsgi_app
        self.scheme = parts.scheme
        self.netloc = parts.netloc
        self.prefix = parts.path.rstrip("/")

    def __call__(self, environ, start_response):
        environ["wsgi.url_scheme"] = self.scheme
        environ["HTTP_HOST"] = self.netloc
        if self.prefix:
            path = environ.get("PATH_INFO", "")
            # A proxy that does not strip the prefix: strip it here so routing still works
            if path == self.prefix or path.startswith(self.prefix + "/"):
                environ["PATH_INFO"] = path[len(self.prefix):] or "/"
            environ["SCRIPT_NAME"] = self.prefix
        return self.wsgi_app(environ, start_response)


def configure_public_url(app, public_url=None, proxy_hops=None):
    """Apply PUBLIC_BASE_URL / TRUSTED_PROXY_HOPS (arguments override the environment)."""
    public_url = (public_url if public_url is not None else os.getenv("PUBLIC_BASE_URL", "")).strip()
    if public_url:
        app.wsgi_app = PublicURLMiddleware(app.wsgi_app, public_url)
        return

    hops = proxy_hops if proxy_hops is not None else int(os.getenv("TRUSTED_PROXY_HOPS", "0") or 0)
    if hops > 0:
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=hops, x_proto=hops, x_host=hops, x_prefix=hops)
