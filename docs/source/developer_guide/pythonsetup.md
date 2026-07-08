# Python setup

(PythonSetup)=
## uv
AMARCORD uses [uv](https://docs.astral.sh/uv/) for managing its dependencies. So either install that and run:

```
uv venv
```

to create a virtual environment with all dependencies installed. Running programs is then simply

```
uv run amarcord-<program-name> <arguments>
```

## Plain pip

Since we have `requirements.txt` files, as long as you don’t want to add new dependencies, you can just create a virtual environment and activate it to get up and running:

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Notes for Microsoft Windows users

We’re using [python-magic](https://pypi.org/project/python-magic/) to determine the type of uploaded files. This depends on `libmagic` which is not available on Windows. You can either do everything using [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) or you can manually `pip install python-magic-bin` which solves the issue.

If you manually created your virtual environment, the way to activate it on Windows is not

``` shell
source venv/bin/activate
```

but rather

``` shell
source venv/Scripts/activate
```

this is for [idiotic reasons](https://stackoverflow.com/questions/43826134/why-is-the-bin-directory-named-differently-scripts-on-windows).

## Notes on the Python code base

### Type checking, formatting, linting, editor support

For **formatting**, we use ruff. Just execute `ruff format amarcord/ tests/` to reformat the whole project.

For **linting** and **type-checking** we use ruff and ty. Just execute `ruff check amarcord tests`, as well as `ty check` (no paths), to lint the whole project.

For **editor support** we currently use basedpyright. We are aware of "ty", but that one doesn't support import completions yet, so we are not using it yet.

### anyio and `Path`

We do use Python's `asyncio` feature a lot. And we also enable ruff's [ASYNC240](https://docs.astral.sh/ruff/rules/blocking-path-method-in-async-function/) check which makes sure to not use `pathlib.Path` functions that might block (the whole program), like `unlink`, `mkdir` and so on. This means:

1. We depend on [anyio](https://pypi.org/project/anyio/) for asynchronous path operations.
2. Whenever we are in an `async def` (an asynchronous function), we convert every `pathlib.Path` into `anyio.Path` and use the appropriate `await`s.
