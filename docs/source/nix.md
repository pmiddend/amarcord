# Nix

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
nix build '.#amarcord-docker-image'
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

## Useful commands

### Check dependencies and closure size

Using

```shell
nix path-info --recursive --size --closure-size --human-readable .#amarcord-production-webserver
```

You can check which packages AMARCORD includes and see if there is anything out of the ordinary. The biggest packages are usually Python and glibc, and that cannot be avoided, really.
