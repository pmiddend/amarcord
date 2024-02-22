# Directory Structure

The following directories are important for understanding the AMARCORD source code:

- `amarcord` — this is where the Python source code resides (i.e. all the `.py` files), see [](BackendCode) for more information.
- `ci` — scripts that are to be called from `.gitlab-ci.yml`, not from the user side; they mostly check the source code
- `docs` — sphinx documentation root; the actual documentation is below `docs/source`, check [](Docs) for more information.
- `frontend` — everything pertaining, you guessed it — the Elm frontend; see [](Frontend) for more information.
- `scripts` — scripts that the user is supposed to call; for example, this contains `scripts/regenerate-requirements.sh` to generate the `pip` `requirements.txt` files
- `stubs` — [type stubs](https://mypy.readthedocs.io/en/stable/stubs.html) for Python libraries that have not types (yet)
- `tests` — home for all pytest tests (see [](BackendTests) for more information on that)
