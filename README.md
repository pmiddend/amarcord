# AMARCORD

## Python setup

### Poetry
AMARCORD uses [Poetry](https://python-poetry.org/) for managing its dependencies. So either install that and run:

```
poetry install
```

to install the dependencies. Running programs is then simply

```
poetry run amarcord-<program-name> <arguments>
```

To get into a shell with the poetry virtual environment:

```
poetry shell
```

where you can use Python to start programs:

```
python amarcord/cli/webserver.py
```

### Plain pip

Since we have `requirements.txt` files, as long as you don’t want to add new dependencies, you can just create a virtual environment and activte it to get up and running:

```
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
```

`requirements-dev.txt` also contains test dependencies and mypy types.

### Notes for Microsoft Windows users

We’re using [python-magic](https://pypi.org/project/python-magic/) to determine the type of uploaded files. This depends on `libmagic` which is not available on Windows. You can either do everything using [WSL](https://docs.microsoft.com/en-us/windows/wsl/install) or you can manually `pip install python-magic-bin` which solves the issue (note that we didn’t include the dependency with the `platform` poetry flag, because that breaks the Nix build).

If you manually created your virtual environment, the way to activate it on Windows isn’t

``` shell
source venv/bin/activate
```

but rather

``` shell
source venv/Scripts/activate
```

this is for [idiotic reasons](https://stackoverflow.com/questions/43826134/why-is-the-bin-directory-named-differently-scripts-on-windows).

## How to start a backend server

To start a web server with a “blank”, but usable SQLite database, run:

```
poetry run amarcord-webserver
```

which will open a web server on port `5000` with an in-memory database, so restarting the server means deleting the database. To get something that sticks around a bit longer:

```
poetry run amarcord-webserver --db-connection-url 'sqlite+aiosqlite:////tmp/database.db'
```

(and yes, there are four slashes in that URL!)

You can also change the port using `--port`.

## How to build and start the frontend

The frontend is written in [Elm](https://elm-lang.org/). Download the `elm` binary at the [Install Elm](https://guide.elm-lang.org/install/elm.html) web site.

To run a *live development environment*, you need [elm-live](https://github.com/wking-io/elm-live) which, unfortunately, needs [node.js](https://nodejs.org/en/). But you can install that really easily on different platforms.

Assuming you’ve got it installed, run:

```
npm install
```

To install the dependencies (you can also use [Yarn](https://yarnpkg.com/), which might be more performant). Then, start a development server via:

```
./run-live-dev-env
```

And point your browser to http://localhost:8000.

You can also just build the latest version without any node.js shenenigans by executing:

```
cd frontend
mkdir output
elm make src/Main.elm --optimize --output output/main.js
cp App.css desy-cfel.png src/index.html output
```

Then run `amarcord-webserver` (as described above) and go to http://localhost:5000/index.html
in your browser.

## Nix

To speed up CI builds and unify dependencies, AMARCORD uses the [Nix package manager](https://nixos.org/). If you have Nix installed, building the AMARCORD Python package is as simple as

``` shell
nix-build -A pythonPackage
```

The `default.nix` file defines a set with a few possible outputs, such as `dockerImage` and `frontend`. You also have a `shell.nix` that you can use for quickly getting a Python development environment with `nix-shell`. The `frontend` directory also has a `shell.nix` with just the Elm compiler in it, right now.

To manage the (pinned) dependency on nixpkgs, we use [niv](https://github.com/nmattia/niv) (and plan to use [flakes](https://nixos.org/manual/nix/stable/command-ref/new-cli/nix3-flake.html) ASAP). Currently, we’re tracking nixpkgs’s `master`, and updating should be as simple as `niv update nixpkgs`.

For the CI build, we have an instance of gitlab-runner on `cfeld-vm04` using the shell executor right now.
