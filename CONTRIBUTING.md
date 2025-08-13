# What to contribute (in general)?

AMARCORD isn't in widespread use, so it is likely that you will encounter rather trivial bugs, typos, or suboptimal UX (User Experience). Thus, _any_ sort of contribution, even if it’s just an issue showing the problem, is very welcome.

# How to contribute

- Post issues in the [GitLab issue tracker](https://gitlab.desy.de/amarcord/amarcord/-/issues).
- Open [merge requests](https://gitlab.desy.de/amarcord/amarcord/-/issues)
  - Try to create one issue per merge request, and link them together
  - If you’re frivolous enough, use [Gitmoji](https://gitmoji.dev) to categorize the issue
  - Prefix your commit message by the subsystem you touch. If it’s the frontend, call it `frontend: did something`, or `webserver: did something else`.

# Code

The continuous integration pipelines should take care of catching any style or code quality issues. In general, take care of installing and using the following tools (per language):

- Python
  - `ruff` to ensure proper _formatting_ and to check the code for issues
  - `basedpyright` to catch bugs and typing issues
  - (optional) `mypy` to catch even more typing issues
  - `fawltydeps` to catch dependency issues
- Elm
  - `elm-review` for general code quality
  - `elm-format` for code style
- Shell scripts
  - `shellcheck` for common isses
  
We use `pytest` and note the code coverage, which shouldn’t degrade too much with the commits you make.
