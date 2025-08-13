# Daemons
## Indexing Daemon
(IndexingDaemon)=

The indexing daemon takes *queued* indexing jobs (queued by the user, or by the API) and executes them on a workload manager, like Slurm. How this works in detail is described in the developer's guide under [](CrystFEL). We will be describing how to start the indexing daemon here. The parameters are:

- `--workload-manager-uri` whose format is described under [](BackendSlurm); the URL specified here is used for *offline* indexing jobs
- `--online-workload-manager-uri` with the same format as the previous argument. This workload manager is used for *online* jobs. The reason this separate parameter exists is because you might have a special reservation for an ongoing beamtime, or have faster nodes available for online indexing, so you can separate it here.
- `--crystfel-path` is the base path to the CrystFEL installation. For example, if it's globally installed, just enter `/usr/bin` here, and the daemon will find the executables as `/usr/bin/indexamajig`.
- `--amarcord-url` is the URL for AMARCORD's API. If AMARCORD and the indexing daemon are on the same host, you can specify `--amarcord-url=http://localhost:8080` here (you must omit the `/api` suffix from the URL). This URL is used to query the queued and ongoing jobs. The daemon does not have a direct database connection, which makes it more versatile.
- `--amarcord-url-for-spawned-job` is, again, the URL to AMARCORD's HTTP API. However, this URL is passed on to the spawned job (on Slurm, for example). This way, you can spawn a job on a remote node and reach AMARCORD via a different URL. For example, you could specify `localhost` as `--amarcord-url`, so the daemon uses this fast connection to get the queued jobs and starts them. On the cluster or cloud then, you specify `--amarcord-url-for-spawned-job=https://public-amarcord.com` and reach the API this way.
- `--asapo-source` is used for online indexing; it is passed to the spawned online jobs to find the data via [ASAP::O](https://asapo.pages.desy.de/asapo/).
- `--overwrite-interpreter` is something you *probably* don't need. The indexing daemon will send the source code for the actual indexing Python script to the workload manager and ask it to execute the transferred source code file. Since this is a Python file, it's important to execute it using the correct path to the Python interpreter, however. Currently, this is hard-coded to `#!/usr/bin/env python3`, which should work for most Linux distributions. It could be that the `python3` thus found is of a wrong version, for example. You can tell the indexing daemon to override this first line in the file, for example like this:
  ```
  --overwrite-interpreter '#!/software/python/3.10/bin/python3'
  ```
- `--gnuplot-path` specifies the absolute path to the `gnuplot` executable to be used for plotting things like the unit cell distributions. You can omit this, and no plotting will be done.
## Merge Daemon
(MergeDaemon)=
