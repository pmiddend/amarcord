class ErrorMessage:
    """
    This type is basically a NewType (see https://mypy.readthedocs.io/en/stable/more_types.html#newtypes) around a str
    which represents an error message. It's a little experiment to test if using that instead of Exceptions for errors
    is a good idea in Python.
    """

    def __init__(self, message: str) -> None:
        self._message = message

    @property
    def message(self) -> str:
        return self._message

    def wrap(self, additional_message: str) -> "ErrorMessage":
        return ErrorMessage(f"{additional_message}: {self._message}")

    def __repr__(self) -> str:
        return self._message

    def __str__(self) -> str:
        return self._message
