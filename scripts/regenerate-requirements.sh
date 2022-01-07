#!/usr/bin/env bash

poetry export --dev --without-hashes > requirements.txt
