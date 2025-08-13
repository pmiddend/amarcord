(OpenAPI)=
# OpenAPI glue

## Rationale

We have a web server with a [REST API](https://en.wikipedia.org/wiki/REST) (or just an *HTTP* API, if you will), and we have both a web [](Frontend) that wants to access this API, as well as a number of [daemons](Executables) to update the database externally.

Of course, we could just make HTTP requests — that's easy both from Python as well as Elm. However, we have to adhere to the API. If we make a HTTP request in the web frontend, and then change the API endpoint in the FastAPI backend, we can only hope we catch this in time and change the frontend as well!

## OpenAPI generator

To eliminate this source of errors, FastAPI exports a so-called [OpenAPI](https://www.openapis.org/) specification, in the form of a JSON file. This JSON file can be used to generate clients (and servers) for lots of different programming languages and libraries using the nice [OpenAPI Generator](https://openapi-generator.tech/) project.

Our workflow in AMARCORD looks like this:

```{mermaid}
    flowchart TD
      Backend[Backend\nwebserver:app] -->|generate_openapi_schema| openapi[openapi.json]
      generator[openapi-generator-cli] --> openapi
      generator --> ElmAPI[API.elm]
```

Generating the Elm code from OpenAPI is thus straightforward: Install the [OpenAPI Generator](https://openapi-generator.tech/) (this varies from operating system to operating system, so we won't describe that here), then execute `scripts/generate-elm-from-openapi.sh` from the root of the AMARCORD directory. The resulting files will be in `frontend/generated`. This directory is set inside `frontend/elm.json` to be one of the source directories.

For the *daemons* and *jobs* started from AMARCORD, as well as external services, we currently hard-code the API and leave it open to errors. In the future, we are going to implement a properly generated OpenAPI client for that as well.
