# Textile API

A Flask-based API for searching and accessing medieval manuscript collections, providing advanced text search capabilities over TEI-encoded documents.

## ✨ Features

- **Full-text search** across manuscript collections with highlighting
- **Manuscript catalog search** with metadata filtering (date, language, location)
- **Hybrid search** combining traditional text matching with vector similarity
- **Date range queries** (e.g., "800-1400" for medieval periods)
- **Export capabilities** (CSV format for research data)
- **TEI document processing** and rendering
- **Elasticsearch integration** for high-performance search
- **Caching system** for improved response times



## 📚 API Endpoints

### Text Search
```http
GET /search/?q=<query>&page=1&size=25&mode=exact
```
- **Modes**: `exact`, `fuzzy`, `partial`
- **Export**: Add `format=csv` for CSV download

### Manuscript Catalog
```http
# General manuscript search
GET /manuscripts/?q=<query>&page=1&size=25

# Search by language
GET /manuscripts/language/?q=<language>&page=1&size=25

# Search by date range
GET /manuscripts/range/?q=800-1400&page=1&size=25

# Search by specific dates
GET /manuscripts/date/?start_year=1200&stop_year=1300
```

### Statistics
```http
# Get total manuscript count
GET /manuscripts/count/

# List all collections
GET /collections/list/
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ELASTICSEARCH_HOST` | Elasticsearch connection URL | `http://dts_elasticsearch:9200` |
| `ELASTICSEARCH_INDEX` | Index name for documents | `documents` |
| `USE_DISK_CACHE` | Enable file-based caching | `true` |
| `PRERENDER_DIR` | Cache directory path | `/app/cache` |
| `DATABASE_PATH` | SQLite database location | `/app/app.db` |


## 🛠 Development

### Local Development

1. **Set up Python environment:**
   ```bash
   python -m venv env
   source env/bin/activate
   pip install -r requirements.txt
   ```

2. **Run Flask development server:**
   ```bash
   export FLASK_APP=app
   flask run
   ```

### Key Dependencies

- **Flask** - Web framework
- **Elasticsearch 8.11.0** - Search engine
- **DaProfileurPyTaInss** - DTS protocol implementation
- **SaxonCHE** - XSL transformations for TEI
- **BeautifulSoup4** - HTML/XML parsing


**Note**: This works with Gunicorn but fails with waitress