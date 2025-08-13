# Quick Start

(QuickStart)=
## Using the Docker image 

If you have Docker installed (or podman) and just want to see "something", you can do that quite easily. First, we create a Docker volume to store the (SQLite in our case) database into:

```
docker volume create --name AmarcordDB
```

Then, we initialize the database:

```
docker run \
  -v AmarcordDB:/db\
  amarcord:latest\
  amarcord-upgrade-db-to-latest\
  --db-connection-url 'sqlite+aiosqlite:////db/test.db'
```

As you can see, we use our `AmarcordDB` volume which we just created, and specify the slightly obtuse database URL as `sqlite+aiosqlite:////db/test.db` (if you're curious: this is telling the [SQLAlchemy](https://pypi.org/project/SQLAlchemy/) library that we want to use sqlite with the [aiosqlite](https://pypi.org/project/aiosqlite/) driver, and that our database is sitting inside `/db/test.db`).

The command should output something along the lines of "everything worked". Now we can start the web server with the API and the GUI attached to it. This command is a bit longer:

```
docker run -v AmarcordDB:/db\
  --env DB_URL='sqlite+aiosqlite:////db/test.db'\
  --publish 8000:8000\
  amarcord:latest\
  amarcord-production-webserver\
  --bind 0.0.0.0:8000
```

Again we're using our magic sqlite URL, and we tell the production webserver to bind to 0.0.0.0 on port 8000, which we expose.

With that set, you can point your browser to http://localhost:8000/index.html and bathe in the glory of the beamtime creation UI.

## Manual

If you don't want to use Docker, you have to do the following:

1. initialize a database (but we can use a local SQLite one, no need to spin up and install heavy servers)
2. build the front-end

Let's do that now, starting with...

### Frontend

The frontend is written in [Elm](https://elm-lang.org/). Download the `elm` binary at the [Install Elm](https://guide.elm-lang.org/install/elm.html) web site.

Having done that, you can build the latest version of the front-end into the `frontend/output` directory (where the web server expects it to be) without any NodeJS shenenigans by executing:

```
cd frontend
mkdir output
elm make src/Main.elm --optimize --output output/main.js
cp -R assets/* src/index.html output/
```

### Backend

To start a backend server, you have to create a database first. This isn't done implicitly when starting the web server, since this created problems. It's easy to create one, however, just do this (yes, there are four `/` in the URL):

```
python amarcord/cli/upgrade_db_to_latest.py --db-connection-url 'sqlite+aiosqlite:////tmp/test.db'
```

To start a web server with a “blank”, but usable SQLite database in `/tmp/test.db` (adapt if you're on Windows), run:

```
DB_URL='sqlite+aiosqlite:////tmp/test.db' uvicorn --port 5000 amarcord.cli.webserver:app
```

which will open a web server on port `5000`.

Then run `amarcord-webserver` (as described above) and go to http://localhost:5000/index.html
in your browser.

You won't see much when you point your browser to http://localhost:5000 though, because we haven't built the front-end yet, and we don't have a prebuilt version of the front-end in the repository. But, read on.

### Local indexing test scenario

There is another test you can perform, or rather, a scenario to create. In it, we create:

- A few run and chemical _Attributi_
- A few runs and chemicals, with the runs pointing to HDF5 files you can specify
- An experiment type which just contains the chemical in the run
- A few data sets based on this experiment type
- A geometry

You can create this scenario by running the `offline_index_test_scenario.py` script. For example, like this:

```
python amarcord/cli/offline_index_test_scenario.py --db-connection-url 'sqlite+aiosqlite:////tmp/test.db' --h5-glob "$HOME/*.h5"
```

This creates an SQLite DB in `/tmp/test.db` and creates a run with its raw files pointing to `$HOME/*.h5`. You can then start the web server with the given `db-connection-url`, and the frontend, and see what you've created.
