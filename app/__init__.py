import os

from flask import Flask, jsonify
from flask_cors import CORS

from dapytains.app.app import create_app

# Base
from .constants import basedir
app = Flask(__name__)

# Load environment variables for configurable origins
# Try to use python-dotenv if available for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv is optional; if not installed, environment variables from the process are used
    pass

# Default CORS origins
default_allowed_origins = [
    "http://localhost:3000",
    "http://localhost:8080", 
    "http://localhost:5173"
]

# Read ALLOWED_ORIGINS from env as comma-separated list. If not set, use defaults.
raw_allowed = os.getenv("ALLOWED_ORIGINS")
if raw_allowed:
    # Split on commas and strip whitespace
    allowed_origins = [o.strip() for o in raw_allowed.split(',') if o.strip()]
else:
    allowed_origins = default_allowed_origins

CORS(app, origins=allowed_origins)

# Database configuration
default_db_path = os.path.join(basedir, 'app.db')
db_path = os.environ.get('DATABASE_PATH', default_db_path)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Importing our customization
from .search_routes import add_search_routes
from .prerendering import CustomXSLTransformer, change_document_route, DiskPrerenderedCache
from .paginated_collection import change_collection_route
from .transformation import media_transformer
from .cli import register_cli

_, db = create_app(app, media_transformer=media_transformer)

from .db_changes import apply_db_changes
apply_db_changes(db)

db.init_app(app)
register_cli(app)
add_search_routes(app)
change_document_route(app, media_transformer)
change_collection_route(app, media_types=sorted(list(media_transformer.supported_media_types)))

# Health check endpoint
@app.route('/health')
def health_check():
    """Health check endpoint for load balancers"""
    try:
        # Check database connection
        with app.app_context():
            from sqlalchemy import text
            db.session.execute(text('SELECT 1'))

        return jsonify({
            'status': 'ok',
            'service': 'dts-api',
            'database': 'connected'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'error',
            'service': 'dts-api',
            'database': 'disconnected',
            'error': str(e)
        }), 500
