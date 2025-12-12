import os

from app import CustomXSLTransformer, basedir

media_transformer = CustomXSLTransformer(
    {"html": os.path.join(basedir, "assets/xsl.xsl")},
    {"text/plain": os.path.join(basedir, "assets/plaintext.xsl")},
)
