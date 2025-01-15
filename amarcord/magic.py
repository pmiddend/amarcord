from pathlib import Path

HARDCODED_MAGIC_VALUE = "application/octet-stream"

try:
    import magic

    def from_file(p: Path, mime: bool) -> str:
        return magic.from_file(str(p), mime=mime)  # type: ignore

    def from_buffer(b: bytes, mime: bool) -> str:
        return magic.from_buffer(b, mime=mime)  # type: ignore

except ImportError:

    def from_file(p: Path, mime: bool) -> str:  # noqa: ARG001
        return HARDCODED_MAGIC_VALUE

    def from_buffer(b: bytes, mime: bool) -> str:  # noqa: ARG001
        return HARDCODED_MAGIC_VALUE
