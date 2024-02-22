# Backend
## Starting a sample web server

To start a backend server, you have to create a database first. This isn't done implicitly when starting the web server, since this created problems. It's easy to create one, however, just do this:

```
python amarcord/cli/upgrade_to_latest.py --db-connection-url 'sqlite+aiosqlite:///tmp/test.db'
```

```{sidebar}
Most of the command-line programs (CLI) in AMARCORD take the `db-connection-url` parameter.
```

To start a web server with a “blank”, but usable SQLite database in `/tmp/test.db` (adapt this path if you're on Windows!), on port 5001, run:

```
DB_URL=sqlite+aiosqlite:////tmp/test.db uvicorn amarcord.cli.webserver:app --port 5001
```

As you can see, the web server isn't start like a normal command-line Python program, but rather managed using [uvicorn](https://www.uvicorn.org/). This allows the web server to use more than one worker process, gaining efficiency. Use `-w 8` as the parameter to `uvicorn` to create 8 "worker processes". For example.

```{sidebar}
If you're wondering about this `sqlite+aiosqlite` syntax: it's a sqlalchemy database URL, see [its docs](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls) for more information. Basically, we're saying "use SQLite, and with the asynchronous aiosqlite driver.
```

(BackendSlurm)=
## Slurm
AMARCORD can start and observe jobs using the [Slurm Workload Manager](https://slurm.schedmd.com/documentation.html). The corresponding code is inside the `amarcord.amici.workload_manager` Python package. There is a (flat) hierarchy of classes in place:

```{mermaid}
classDiagram
	WorkloadManager <|-- DummyWorkloadManager
    WorkloadManager <|-- LocalWorkloadManager
    WorkloadManager <|-- SlurmRemoteWorkloadManager
    WorkloadManager <|-- SlurmRestWorkloadManager

    class WorkloadManager{
       start_job(...) : JobStartResult
       list_jobs() -> Iterable[Job]
    }
```

The available managers are:

- **DummyWorkloadManager** is used in tests only. It keeps an in-memory list of started jobs so you can test for those. `list_jobs` returns this list.
- **LocalWorkloadManager** uses Python's [subprocess](https://docs.python.org/3/library/subprocess.html) library to start jobs on the local machine.
- **SlurmRemoteWorkloadManager** uses `asyncio`'s [subprocess API](https://docs.python.org/3/library/asyncio-subprocess.html) to run `sbatch` via SSH.
- **SlurmRestWorkloadManager** uses the [Slurm REST API](https://slurm.schedmd.com/rest.html).

To *create* a workload manager, you shouldn't instantiate it directly, and that's not how AMARCORD does it. Instead, there is a function that receives a *parsed workload manager URI* and gives you a reference to a `WorkloadManager` back. This function is located in `amarcord.amici.workload_manager.workload_manager_factory`. In general, the *workload manager URI* looks like this:

```
manager-type:foo=bar|baz=qux
```

It starts with the manager type, which is one of `local`, `petra3-slurm-remote`, `maxwell-rest` or `slurm-rest`. Depending on the concrete manager, you to specify certain configuration parameters. Specifically:

- `local`: takes no parameters.
- `petra3-slurm-remote`: takes a `beamtime-id`, a `path` and `use-additional-ssh-options`; see the source code for more information
- `maxwell-rest` is a special case of `slurm-rest` with the URL-based parameters hard-coded for DESY's [Maxwell Cluster](https://confluence.desy.de/display/MXW/Maxwell+Cluster).
- `slurm-rest` takes the following parameters:
  - `use-http` (bool, use `true` and `false`, default `false`) if you want to use `http` instead of the default `https`
  - `partition` (string, optional) is the Maxwell partition to use
  - `host` (string) is the host to use for the HTTP URL
  - `port` (int) which port to use
  - `path` (string) which path to append to the URL at the end
  - `user` (string, default `getuser()`) the user name to use in the header
  - `reservation` (string, optional) the Slurm reservation to use
  - `explicit-node` (string, optional) the Slurm node to use
  - `token` (string, optional) the token to use

(BackendCode)=
(CrystFEL)=
## CrystFEL interaction
FIXME
## Code

(DatabaseCode)=
### Logging
FIXME
### Database

See [](Database) for an in-depth introduction into the layout of the database. This section will document the Python code to interface with it.

We are using the excellent [SQLAlchemy](https://www.sqlalchemy.org/) library to access our SQL database backend. There are two "variants" of the library: core and ORM. Currently, we are using core. Using the ORM is a work-in-progress. This means that our database interface class, `AsyncDB`, does raw SQL queries using the core, and converts them into Python structures, mostly defined in the `table_classes.py` file by hand. No automatic mapping between columns and Python fields done. Our "mapped" classes are prefixed with `DB`. Sometimes there is an `...Output` and an `...Input` variant. The latter is used for creating an entity and thus usually doesn't have an `id` field. The former is for already-created entities.

For ID columns, sometimes we're using a [NewType](https://mypy.readthedocs.io/en/stable/more_types.html) wrapper, so different integral IDs can be better differentiated. Compare:

```python
def f(beamtime_id: int, run_id: int) -> None:
   ...
   
f(1,3)
f(6,7)
```

with:

```python
BeamtimeId = NewType("BeamtimeId", int)
RunId = NewType("RunId", int)

def f(beamtime_id: BeamtimeId, run_id: RunId) -> None:
   ...
   
f(BeamtimeId(1), RunId(3))
f(BeamtimeId(6), RunId(7))
```

### Attributi

Attributi are represented by two main classes: `DBAttributo` represents one attributo in the database, with its name, type, and so on. `AttributiMap` is a wrapper around a collection of attributo values. In other words `DBAttributo` represents an attributo *schema* while `AttributiMap` represents attributi *values*. The `AttributiMap` is type-safe, so you are not allowed to assign a string value to an integer attributo. This is achieved by passing in not just the attributo values (as pairs of the attributo ID and its value), but also the list of `DBAttributo` classes.

### Web server

The web server is located below `amarcord/cli/webserver.py` and uses [FastAPI](https://fastapi.tiangolo.com/) to define the HTTP endpoints. This means you have to be familiar with this library, and also [pydantic](https://docs.pydantic.dev/1.10/) which is used for the JSON serialization.

(BackendTests)=
### Tests

### Unused libraries: `fawltydeps`

To check for unused libraries, or *used* libraries that are not declared (and are brought in transitively), we use [fawltydeps](https://github.com/tweag/FawltyDeps) in a CI step of the same name.

### Unused code: `vulture`

Every once in a while, we run `vulture` (see <https://github.com/jendrikseipp/vulture>) to eliminate dead code in the application. Due to some libraries being a bit more dynamic, we have to manually inspect the output and remove things.
