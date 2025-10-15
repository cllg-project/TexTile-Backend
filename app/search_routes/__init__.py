# Add vector search capability
# import warnings
# warnings.filterwarnings('ignore')

# try:
#     from sentence_transformers import SentenceTransformer
#     import numpy as np
#     VECTOR_SEARCH_AVAILABLE = True
#     print("✅ Vector search available")
#     # Initialize model (do this once when app starts)
#     _vector_model = None
#
#     def get_vector_model():
#         global _vector_model
#         if _vector_model is None:
#             print("Loading sentence transformer model...")
#             _vector_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
#         return _vector_model
#
# except ImportError:
# print("❌ Vector search not available - will be available soon")


# def _generate_semantic_highlights(query, content, model):
#     """
#     Generate semantic highlights by finding phrases in content that are most similar to the query.
#     Returns highlighted content and list of matched phrases.
#     """
#     if not model or not content or not query:
#         return content, []
#
#     try:
#         import re
#
#         # Split content into sentences and phrases
#         sentences = re.split(r'[.!?]+', content)
#         phrases = []
#
#         # Create overlapping phrases of different lengths (2-8 words)
#         for sentence in sentences:
#             words = sentence.strip().split()
#             if len(words) < 2:
#                 continue
#
#             # Add phrases of different lengths
#             for length in [2, 3, 4, 5, 6, 7, 8]:
#                 for i in range(len(words) - length + 1):
#                     phrase = ' '.join(words[i:i+length])
#                     if len(phrase.strip()) > 5:  # Ignore very short phrases
#                         phrases.append(phrase.strip())
#
#         # Also add individual meaningful words (longer than 3 characters)
#         words_in_content = re.findall(r'\b\w{4,}\b', content)
#         phrases.extend(words_in_content[:50])  # Limit to avoid too many embeddings
#
#         if not phrases:
#             return content, []
#
#         # Get embeddings for query and phrases
#         query_embedding = model.encode([query])
#         phrase_embeddings = model.encode(phrases[:100])  # Limit to first 100 phrases for performance
#
#         # Calculate similarities
#         from sentence_transformers.util import cos_sim
#         similarities = cos_sim(query_embedding, phrase_embeddings)[0]
#
#         # Find top matching phrases (similarity > 0.4)
#         matched_phrases = []
#         for i, similarity in enumerate(similarities):
#             if similarity > 0.4 and i < len(phrases):  # Reasonable similarity threshold
#                 matched_phrases.append({
#                     'phrase': phrases[i],
#                     'similarity': float(similarity)
#                 })
#
#         # Sort by similarity and take top 5
#         matched_phrases.sort(key=lambda x: x['similarity'], reverse=True)
#         matched_phrases = matched_phrases[:5]
#
#         # Highlight the matched phrases in the content
#         highlighted_content = content
#         for match in matched_phrases:
#             phrase = match['phrase']
#             # Use word boundary regex to avoid partial matches
#             pattern = r'\b' + re.escape(phrase) + r'\b'
#             highlighted_content = re.sub(
#                 pattern,
#                 f"<mark class='vector-match' data-similarity='{match['similarity']:.3f}'>{phrase}</mark>",
#                 highlighted_content,
#                 flags=re.IGNORECASE
#             )
#
#         return highlighted_content, [m['phrase'] for m in matched_phrases]
#
#     except Exception as e:
#         # Fallback: return original content without highlighting
#         return content, []


def add_search_routes(app):
    from .catalog_search import manuscripts_route, manuscripts_language_route, manuscripts_date_route, manuscripts_range_route
    from .text_search import search_route, hybrid_search_route
    from .manuscript_stats import manuscripts_count_route, collections_list_route
    app.add_url_rule('/search/', 'search_route', search_route)
    app.add_url_rule("/manuscripts/date/", "manuscripts_date_route", manuscripts_date_route)
    app.add_url_rule("/manuscripts/language/", "manuscripts_language_route", manuscripts_language_route)
    app.add_url_rule("/manuscripts/range/", "manuscripts_range_route", manuscripts_range_route)
    app.add_url_rule("/manuscripts/count/", "manuscripts_count_route", manuscripts_count_route)
    app.add_url_rule("/collections/list/", "collections_list_route", collections_list_route)
    app.add_url_rule("/search/hybrid/", "hybrid_search_route", hybrid_search_route)
    app.add_url_rule("/manuscripts/", "manuscripts_route", manuscripts_route)
