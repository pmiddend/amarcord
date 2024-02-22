# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "AMARCORD"
copyright = "2024, Philipp Middendorf"
author = "Philipp Middendorf"
release = "1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# see https://www.sphinx-doc.org/en/master/usage/markdown.html
extensions = ["sphinxcontrib.mermaid", "myst_parser", "sphinxcontrib.spelling"]

myst_fence_as_directive = ["mermaid"]

templates_path = ["_templates"]
exclude_patterns = []

# This is weirdly underdocumented, I only found this here:
# https://github.com/sphinx-doc/sphinx/issues/521
linkcheck_ignore = ["http://localhost.*"]

spelling_lang = "en_US"

html_logo = "./amarcord-logo-smaller.png"
# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
