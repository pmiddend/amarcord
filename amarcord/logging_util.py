import os
import sys


def setup_structlog() -> None:
    import structlog
    from structlog.processors import JSONRenderer
    from structlog.processors import TimeStamper
    from structlog.processors import format_exc_info
    from structlog.stdlib import add_log_level
    from structlog_overtime import TeeLoggerFactory
    from structlog_overtime import TeeOutput

    structlog.configure(
        logger_factory=TeeLoggerFactory(
            TeeOutput(
                processors=[structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())],
                logger_factory=structlog.PrintLoggerFactory(sys.stderr),
            ),
            TeeOutput(
                processors=[structlog.dev.ConsoleRenderer(colors=False)],
                logger_factory=structlog.PrintLoggerFactory(
                    open(  # pylint: disable=consider-using-with
                        f"{os.environ.get('LOG_OUTPUT_FILE_PREFIX', 'output')}.txt",
                        "a",
                        encoding="utf-8",
                    )
                ),
            ),
            TeeOutput(
                processors=[JSONRenderer(sort_keys=True)],
                logger_factory=structlog.PrintLoggerFactory(
                    open(  # pylint: disable=consider-using-with
                        f"{os.environ.get('LOG_OUTPUT_FILE_PREFIX', 'output')}.json",
                        "a",
                        encoding="utf-8",
                    )
                ),
            ),
        ),
        processors=(
            [
                TimeStamper(fmt="iso"),
                add_log_level,
                format_exc_info,
            ]
        ),
    )
