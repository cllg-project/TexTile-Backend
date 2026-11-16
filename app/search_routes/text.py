import unicodedata


def fold(text: str) -> str:
    """Lowercase and strip diacritics, like the index's folded_analyzer."""
    decomposed = unicodedata.normalize("NFD", (text or "").lower())
    return "".join(c for c in decomposed if not unicodedata.combining(c))
