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

To start a backend server, you have to create a database first. This isn't done implicitly when starting the web server, since this created problems. It's easy to create one, however, just do this:

```
python amarcord/cli/upgrade_to_latest.py --db-connection-url 'sqlite+aiosqlite:///tmp/test.db'
```

To start a web server with a “blank”, but usable SQLite database in `/tmp/test.db` (adapt if you're on Windows), run:

```
DB_URL=sqlite+aiosqlite:////tmp/test.db 
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
./run-live-dev-env.sh
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

To speed up CI builds and unify dependencies, AMARCORD uses the [Nix package manager](https://nixos.org/). It also uses flakes, so enable those in your `~/.config/nix/nix.conf`:

```
experimental-features = nix-command flakes
```

If you have Nix installed, building the AMARCORD Python package is as simple as

```shell
nix build '.#amarcord-python-package'
```

To build a Docker container:

```shell
nix build '.#amarcord-docker-image
```

To get a development shell with the Python dependencies:

```shell
nix develop
```

To get a development shell with the Elm dependencies:

```shell
cd frontend
nix develop '..#frontend'
```

For the CI build, we have an instance of gitlab-runner on `cfeld-vm04` using the shell executor right now.
