#!/usr/bin/env bash

set -ex

gunicorn --bind 0.0.0.0:5000 amarcord.cli.webserver:app
