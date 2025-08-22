import os
import sys

sys.path.insert(0, os.path.abspath(".."))  # Source code dir relative to this file

import terragon  # noqa: E402

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "terragon"
copyright = "2024, Adrian Höhl"
author = "Adrian Höhl"
version = terragon.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # automatically generating documentation from docstrings
    "nbsphinx",  # converting notebooks
    "myst_parser",  # markdown parser
    "sphinx.ext.napoleon",  # support for numpy and google docstrings
    "sphinx_autodoc_typehints",  # Automatically document type hints
]

# nbsphinx_html = True
nbsphinx_execute = "never"  # Options: 'always', 'never', or 'auto'
nbsphinx_timeout = 60  # Timeout in seconds
nbsphinx_allow_errors = True  # Continue building even if a cell errors

typehints_fully_qualified = True  # Show full path for types
autodoc_typehints = (
    "description"  # "signature" places them inline, "description" adds a separate section
)

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# mock external modules using requirements.txt
autodoc_mock_imports = ["ee"]  # add others manually if needed
with open("../requirements.txt") as f:
    requirements = f.read().splitlines()
for requirement in requirements:
    if "#" in requirement:
        continue
    if "==" in requirement:
        requirement = requirement.split("==")[0]
    autodoc_mock_imports.append(requirement)
    if "-" in requirement:
        autodoc_mock_imports.append(requirement.replace("-", "_"))

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_logo = "_static/logo.png"

# Material things
html_theme_options = {
    "logo_only": False,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False,
    "vcs_pageview_mode": "",
    "style_nav_header_background": "&#xe88a",
    "flyout_display": "hidden",
    "version_selector": True,
    "language_selector": True,
    # Toc options
    "collapse_navigation": False,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "includehidden": True,
    "titles_only": False,
}
