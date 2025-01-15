import os
from pathlib import Path

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from amarcord.logging_util import setup_structlog
from amarcord.web.router_analysis import router as analysis_router
from amarcord.web.router_attributi import router as attributi_router
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
app.include_router(indexing_router)
app.include_router(merging_router)
app.include_router(misc_router)
app.include_router(runs_router)
app.include_router(schedule_router)
app.include_router(spreadsheet_router)
app.include_router(user_configuration_router)

# Neat trick: mount this after the endpoints to have both static files under / (for example, /index.html) as well as API endpoints under /api/*
# Credits to https://stackoverflow.com/questions/65419794/serve-static-files-from-root-in-fastapi
real_static_folder = os.environ.get(
    "AMARCORD_STATIC_PATH",
    str(Path.cwd() / "frontend" / "output"),
)
if Path(real_static_folder).is_dir():
    app.mount(
        "/",
        StaticFiles(
            directory=real_static_folder,
            html=True,
        ),
        name="static",
    )
