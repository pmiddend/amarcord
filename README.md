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

`requirements-dev.txt` also contains test dependencies and mypy types

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
mkdir output
elm make src/Main.elm --optimize --output output/main.js
cp App.css index.html output
```

And then opening `index.html` in your browser.
