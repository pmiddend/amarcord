#!/usr/bin/env bash

set -eu
set -o pipefail

curl --include\
     -X POST\
     --header "Content-Type: multipart/form-data"\
     --form "blob=@$1;type=image/jpeg"\
     http://localhost:5000/api/live-stream
