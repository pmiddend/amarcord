(Docs)=
# Documentation

## General

Documentation is written using [Sphinx](https://www.sphinx-doc.org/en/master/). Since Markdown is a bit more common these days, we're using [Markdown](https://en.wikipedia.org/wiki/Markdown) for everything except `index.rst`, which (we think) still has to be in [reStructuredText](https://en.wikipedia.org/wiki/ReStructuredText).

To build the documentation, install the development (poetry group `dev`, and `requirements-dev.txt`) requirements (for example using [poetry](PoetrySetup)), then `cd` into `docs` and run `make html` to create `build/html/index.html` and its companion files. You can open this file using the browser.

## Code documentation

For some things in the code, there are Python [docstrings](https://peps.python.org/pep-0257/) scattered around, but the main source of information will always be this document, as it gives a much more high-level overview.

## Automatically reloading documentation

To write documentation and immediately see the results, we use [sphinx-autobuild](https://github.com/sphinx-doc/sphinx-autobuild). Simply `cd` into `docs` and then run 

```
sphinx-autobuild source/ docs/_build/html/
```

And point your browser to [http://localhost:8000](http://localhost:8000).

## Spelling

We use [sphinxcontrib-spelling](https://sphinxcontrib-spelling.readthedocs.io/en/latest/) to check the documentation's spelling. If you have a word that you know is correct, but is not in the dictionary, just add it to the file `docs/spelling_wordlist.txt` (it’s one word per line).

To check spelling explicitly, run

```
sphinx-build -b spelling source/ build/
```
