import os
from pathlib import Path

import structlog
from fastapi import FastAPI
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.types import Scope

from amarcord.logging_util import setup_structlog
from amarcord.web.router_analysis import router as analysis_router
from amarcord.web.router_attributi import router as attributi_router
from amarcord.web.router_geometry import router as geometry_router
from amarcord.web.router_beamtimes import router as beamtimes_router
from amarcord.web.router_chemicals import router as chemicals_router
from amarcord.web.router_data_sets import router as data_sets_router
from amarcord.web.router_events import router as events_router
from amarcord.web.router_experiment_types import router as experiment_types_router
from amarcord.web.router_files import router as files_router
from amarcord.web.router_indexing import router as indexing_router
from amarcord.web.router_merging import router as merging_router
from amarcord.web.router_misc import router as misc_router
from amarcord.web.router_runs import router as runs_router
from amarcord.web.router_schedule import router as schedule_router
from amarcord.web.router_spreadsheet import router as spreadsheet_router
from amarcord.web.router_user_configuration import router as user_configuration_router

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)


hardcoded_static_folder: None | str = None

app = FastAPI(title="AMARCORD OpenAPI interface", version="1.0")
origins = [
    "http://localhost:5001",
    "http://localhost:8001",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# please sort the following lines alphabetically when changing them #channelyourinnermonk
app.include_router(analysis_router)
app.include_router(attributi_router)
app.include_router(beamtimes_router)
app.include_router(chemicals_router)
app.include_router(data_sets_router)
app.include_router(events_router)
app.include_router(experiment_types_router)
app.include_router(files_router)
app.include_router(geometry_router)
app.include_router(indexing_router)
app.include_router(merging_router)
app.include_router(misc_router)
app.include_router(runs_router)
app.include_router(schedule_router)
app.include_router(spreadsheet_router)
app.include_router(user_configuration_router)


# This needs some explanation:
#
# By default, the browser is free to cache web sites, images, ...,
# arbitrarily long. This is good, but also annoying if you want the
# client to please update the Javascript files to the latest version.
# Previously, the client just errored, and we displayed an alert
# telling the user to please reload, clearing the cache. This
# rightfully pissed everyone off to no end, so the solution is now as
# follows:
#
# - Set the index.html file to "browser, do not cache this!" (index.html is tiny anyways)
# - In index.html, don't include "main.js", but "main-$hash.js", where $hash is computed from the contents of "frontend/src"
#
# This way, the browser always sees the latest version of main.js, and
# can even cache that one. Just index.html is reloaded every time
# (which is fine).
#
# Thanks to SO:
#
# https://stackoverflow.com/a/2068407
class CacheControlledStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope: Scope) -> Response:
        if "index.html" not in path:
            return await super().get_response(path, scope)

        # Prevent starlette/fastapi from just returning "everything still valid, 304!"
        scope["headers"] = [
            (k, v)
            for k, v in scope["headers"]
            if k not in (b"If-Modified-Since", b"If-None-Match")
        ]
        response = await super().get_response(path, scope)
        # Prevent the browser from caching
        # HTTP 1.1
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        # HTTP 1.0 and ancient clients
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["Vary"] = "*"
        return response


# Neat trick: mount this after the endpoints to have both static files under / (for example, /index.html) as well as API endpoints under /api/*
# Credits to https://stackoverflow.com/questions/65419794/serve-static-files-from-root-in-fastapi
real_static_folder = os.environ.get(
    "AMARCORD_STATIC_PATH",
    str(Path.cwd() / "frontend" / "output"),
)
if Path(real_static_folder).is_dir():
    app.mount(
        "/",
        CacheControlledStaticFiles(
            directory=real_static_folder,
            html=True,
        ),
        name="static",
    )
