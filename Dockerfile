FROM python:3.8-slim

RUN mkdir /app
COPY . /app
WORKDIR /app

ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PATH="$PATH:/root/.poetry/bin"
RUN pip install 'poetry==1.1.*'
RUN poetry install --no-interaction --no-dev

ENV AMARCORD_STATIC_FOLDER=/app/frontend/output
