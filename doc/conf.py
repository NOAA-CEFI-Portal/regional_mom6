import os
import sys
sys.path.insert(0, os.path.abspath('../mom6/'))

# # Project information
# project = "Regional MOM6"
# author = "regional_mom6 code dev team"
# release = "0.1.0"

# Extensions
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # Support for NumPy/Google-style docstrings
    "sphinx.ext.viewcode",  # Link to source code
]

# autodoc settings
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
}

# # Paths for templates and static files
# templates_path = ['_templates']
# exclude_patterns = []

# # HTML theme
# html_theme = "sphinx_book_theme"
