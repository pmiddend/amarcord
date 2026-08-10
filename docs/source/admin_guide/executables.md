(Executables)=
# Executables Overview

## `amarcord-slurm-runner`

Located in `amarcord/cli/slurm_runner.py`.

Executes a script on a remote machine using [Slurm](https://slurm.schedmd.com/documentation.html). This is just a testing tool for the Slurm backend in AMARCORD. You can pass a so-called workload manager URL and the script to run, as well as some other job metadata, and the tool will run and observe the job until it finishes (either erroneously or successfully). See [our Slurm documentation](BackendSlurm) for more information.

## `amarcord-upgrade-db-to-latest`

Located in `amarcord/cli/upgrade_db_to_latest.py`.

This just takes a database URL and will run all migrations, in order to upgrade the database to the latest version (see [](Alembic) for an explanation on the migration concept). In the case of [SQLite](https://www.sqlite.org/index.html) this can also *create* a new database.

## `amarcord-generate-openapi-schema`

Located in `amarcord/cli/generate_openapi_schema.py`.

Generate an `openapi.json` file to use, for example, to generate Elm code for the frontend. See the [OpenAPI section](OpenAPI) section for more information.

## `amarcord-indexing-daemon`

Located in `amarcord/cli/indexing_daemon.py`.

Daemon which retrieves queued indexing jobs from the DB and starts them on the workload manager. See [](IndexingDaemon) for detailed information.

## `amarcord-merge_daemon`

Located in `amarcord/cli/merge_daemon.py`.

Daemon which retrieves queued merge jobs from the DB and starts them on the workload manager. See [](MergeDaemon) for detailed information.

## `crystfel_merge.py`

Not really an executable you're supposed to call directly. This is transferred via [Slurm](BackendSlurm) to a remote node and executed there to do merging and refining with [CrystFEL](https://www.desy.de/~twhite/crystfel/) and accompanying tools, see [](CrystFEL) for more information.

## `crystfel_index.py`

Same as for `crystfel_merge`, see [](CrystFEL) for more information.

## `id29_push_daemon.py`

See [](ID29) for more information.

## `id29_pull_daemon.py`

See [](ID29) for more information.
