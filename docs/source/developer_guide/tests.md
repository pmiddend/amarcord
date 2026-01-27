# Tests

## General

We're using [pytest](https://docs.pytest.org/en/latest/) for Python-based tests. You can start them by executing

```
pytest tests
```

## Web Server Tests

Below `tests/test_webserver.py` you can find tests for the FastAPI web server. They construct a real SQLite database and test the web server endpoints using the pydantic schema classes, to ensure maintainability and correctness. The [FastAPI docs](https://fastapi.tiangolo.com/de/tutorial/testing/) have all the information needed to understand testing with FastAPI. Essentially, the `TestClient` object gives you HTTP methods to call your API with.
