#!/usr/bin/env bash

rm -f /tmp/newnewtest.db && FLASK_APP=amarcord/xfel_webserver.py AMARCORD_CREATE_SAMPLE_DATA="y" flask run
