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
### Jobs on Slurm

Currently, AMARCORD only supports the [Slurm](https://slurm.schedmd.com/documentation.html) workload manager to offload jobs to, for example, DESY's [Maxwell](https://maxwell.desy.de/) cluster. The preferred way to interact with Slurm is via the REST interface (see above).

The general idea behind CrystFEL + jobs in AMARCORD is that you have a _daemon process_ that takes care of starting and observing jobs on other machines. The daemons can run on persistently running, slow machines, while the started jobs are ephemeral and have to run fast.

In order to accomplish this separation of domains, AMARCORD sends the script to be executed on the computation nodes through the Slurm "start job" request. Input parameters to the scripts are communicated via environment variables, which can be also passed in the "start job" request. Results and status from these scripts is transported back to AMARCORD via its HTTP REST API. The scripts that run on the computation nodes are just normal Python scripts. It is imperative, though, that these scripts are self-contained, i.e. have no dependencies on other AMARCORD modules, and ideally no Python modules other than the standard library.

The following diagram illustrates what we do:

```{mermaid}
sequenceDiagram
    participant Daemon Process
    Daemon Process ->> Slurm REST: Start job please
    create participant Computation Job
    Slurm REST ->> Computation Job: Creates
    Computation Job ->> AMARCORD REST: Status 50%
    Computation Job ->> File system: Write output files
    Computation Job ->> AMARCORD REST: Completed
    destroy Computation Job
```

In addition to _starting_ jobs, the daemon processes also observe jobs via the Slurm REST interface and mark jobs as running, as well as completed (if, for example, the job exited prematurely).

```{mermaid}
sequenceDiagram
	participant DB
	participant AMARCORD REST
    participant Daemon Process
    participant Slurm REST
    Daemon Process ->> AMARCORD REST: Are there jobs queued?
	AMARCORD REST ->> DB: Any jobs queued?
	DB -->> AMARCORD REST: Sure, here is one with ID 8001!
	AMARCORD REST -->> Daemon Process: Here's one with ID 8001!
	Daemon Process ->> Slurm REST: Start
	Slurm REST -->> Daemon Process: Started, ID 31337
    Daemon Process ->> Slurm REST: How about job 31337?
	Slurm REST -->> Daemon Process: Sorry, what?
	Daemon Process ->> AMARCORD REST: Job for ID 8001 (SLURM ID 31337) crashed or something
	AMARCORD REST ->> DB: Set 8001 to crashed
```

### Distinction between Online and "Offline" Indexing

In general, experiments implement one of the following "data flows":

```{mermaid}
flowchart TD
    Detector --> ImagesOnDisk[Images on Disk] -->|indexamajig| StreamFiles[CrystFEL Stream Files] -->|partialator| HklFiles[CrystFEL hkl files] -->|dimple| PdbFiles[PDB files]
	Detector -->|CrystFEL online| StreamFiles
```

The left path (the one with images being stored on disk) is what we call "offline indexing" in this document. The other path is called online indexing. In both cases, we convert images to indexed stream files, which can then be merged into hkl files (which, in turn, can be turned into MTZ files if needed) and then phased and refined into pdb files.

### Offline Indexing

The core of both “online” and "offline" indexing (as described above) consists of a single Python script: `crystfel_index.py`. This script is spawned on a computation node by AMARCORD and is responsible for spawning secondary processes, observing them, and communicating their results back to AMARCORD. The secondary processes are responsible for the actual indexing work. These processes spawn an `indexamajig` subprocess and observe it.

Primary and secondary processes communicate via a shared [SQLite](https://sqlite.org) database. It is thus assumed that all processes share a file-system structure, and concurrent reading/writing from that file system structure is fast.

One twist is that both primary and secondary processes are in the same Python script. When it is run, an environment variable is checked to see if it's supposed to be run in primary or secondary mode.

```{mermaid}
flowchart TD
    OffIndexPrim["`crystfel_index.py in **primary** mode`"]
    OffIndexSec["`crystfel_index.py in **secondary** mode`"]
	SQLite["SQLite database"]
    AMARCORD -->|1. Start via Slurm| OffIndexPrim
	OffIndexPrim -->|2. Initialize with job info| SQLite
	OffIndexPrim -->|3. Start via Slurm| OffIndexSec
	OffIndexSec -->|4. Write status| SQLite
	SQLite -->|5. Read status| OffIndexPrim
	OffIndexPrim -->|6. Write status| AMARCORD
```

Input arguments, such as the indexamajig parameters and the AMARCORD URL, are passed to `crystfel_index.py` via environment variables, and possibly passed along the primary → secondary route.

The input files for the indexing job are specified as a [glob](https://en.wikipedia.org/wiki/Glob_(programming)) from the database (usually in the form `beamtime folder/raw/run_1337/*h5` or similar). The indexing script resolves this first to a list of files, and then, via CrystFEL's [list_events](https://www.desy.de/~twhite/crystfel/manual-list_events.html) program, into a list of events inside these files.

Then, the list of events is made into *batches* of a specified, fixed number of images (say 1000 images per batch). For each of these batches, a new secondary offline indexing job is started, which, in turn, starts `indexamajig` on the specified events.

```{mermaid}
sequenceDiagram
    Primary ->> ImageBatch: create
	Secondary ->> ImageBatch: read from
	Secondary ->> SmallStreamFile: produce via indexamajig
	Primary ->> SmallStreamFile: read
	Primary ->> LargeStreamFile: concatenate into
```

This can result in a large number of jobs to start. To make this easy on the Slurm scheduler, instead of starting single jobs, [Job Arrays](https://slurm.schedmd.com/job_array.html) are used. The idea behind these is simple: give Slurm a script to run, and the number of times to run it (giving a range, say 1-300). Slurm will then run the script 300 times (in our example), each time with the same input (same environment variables, same script), but with a different value for the environment variable `SLURM_ARRAY_TASK_ID` (ranging from 1 to 300 in our example). The user is then responsible to figure out the corresponding input values for the script. This is easy for `crystfel_index.py` though, since there's a SQLite database containing all this information.

When an offline indexing job is created, using the HTTP API, AMARCORD looks at files associated with a run (i.e. the `RunHasFiles` table), and treats that as a glob pattern to search for input files for offline indexing.

## Code

(DatabaseCode)=
### Logging
FIXME
### Database

See [](Database) for an in-depth introduction into the layout of the database. This section will document the Python code to interface with it.

We are using the excellent [SQLAlchemy](https://www.sqlalchemy.org/) library to access our SQL database backend. There are two "variants" of the library: core and ORM. We are using the ORM. The ORM classes are all located in a single module `amarcord.db.orm`. We are using `MappedAsDataclass` so we have full type-checking available, even for `__init__`.

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
