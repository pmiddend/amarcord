#!/usr/bin/env bash

curl --include\
     -X POST\
     --header "Content-Type: multipart/form-data"\
     --form "blob=@./testfile.txt;type=text/plain"\
     --form "metadata={\"description\": \"just a test\"};type=application/json"\
     http://localhost:5000/api/files
