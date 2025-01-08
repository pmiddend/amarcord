# Python setup

(PythonSetup)=
## Poetry
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

We’re using [python-magic](https://pypi.org/project/python-magic/) to determine the type of uploaded files. This depends on `libmagic` which is not available on Windows. You can either do everything using [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) or you can manually `pip install python-magic-bin` which solves the issue (note that we did not include the dependency with the `platform` poetry flag, because that breaks the Nix build).

If you manually created your virtual environment, the way to activate it on Windows is not

``` shell
source venv/bin/activate
```

but rather

``` shell
source venv/Scripts/activate
```

this is for [idiotic reasons](https://stackoverflow.com/questions/43826134/why-is-the-bin-directory-named-differently-scripts-on-windows).
