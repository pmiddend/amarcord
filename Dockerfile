# bullseye is actually important here: we want to use certain binaries (e.g. for asyncmy) and
# these depend on the glibc version used. 3.9-slim at time of writing had a version that's too low.
FROM python:3.9-slim-bullseye

RUN mkdir /app
COPY . /app
WORKDIR /app

RUN apt-get update && apt-get install --no-install-recommends -y libmagic1 && apt-get clean && rm -rf /var/lib/apt/lists/*

# check glibc version
RUN ldd --version
# --only-binary because we don't want to build anything in the container, this would blow up its size
RUN pip install  --only-binary ':all:' --no-binary 'randomname' --no-binary 'termcolor' --no-binary 'fire' --no-binary 'blinker' --no-binary 'typed-argument-parser' -r requirements.txt

ENV PYTHONPATH="/app:$PYTHONPATH"
ENV AMARCORD_STATIC_FOLDER=/app/frontend/output
