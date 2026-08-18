import asyncio
import datetime

import aiohttp
import structlog
from typed_argparse import TypedArgs
from typed_argparse import arg

from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_stream_loop
from amarcord.db.attributi import utc_int_to_utc_datetime
from amarcord.web.json_models import JsonReadBeamtime

logger = structlog.stdlib.get_logger(__name__)


class Arguments(TypedArgs):
    amarcord_url: str = arg(
        help="URL the daemon uses to look up indexing jobs in the DB"
    )
    stream_url: str = arg(help="URL to the mjpeg stream to grab frames from")
    beamline_filter: str = arg(help="Can be used to filter by beamline")
    delay_seconds: float = arg(
        default=5.0, help="How often (in seconds delay) to shoot a picture"
    )


async def _mjpeg_stream_loop(args: Arguments) -> None:
    logger.info("starting mjpeg loop")
    async with aiohttp.ClientSession() as session:
        current_mjpeg_loop = None

        previous_failure = False
        while True:
            try:
                async with session.get(f"{args.amarcord_url}/api/beamtimes") as resp:
                    # our loop could have just crashed while we were sleeping or doing the request.
                    # treat this as if the loop is not in existence
                    if current_mjpeg_loop is not None and current_mjpeg_loop.done():
                        current_mjpeg_loop = None

                    beamtimes = JsonReadBeamtime(**(await resp.json()))

                    now = datetime.datetime.now(datetime.UTC)
                    current_beamtime = None
                    for beamtime in beamtimes.beamtimes:
                        if (
                            utc_int_to_utc_datetime(beamtime.start)
                            < now
                            < utc_int_to_utc_datetime(beamtime.end)
                            and beamtime.beamline.strip().lower()
                            == args.beamline_filter.strip().lower()
                        ):
                            current_beamtime = beamtime

                    if current_beamtime is None:
                        if current_mjpeg_loop is not None:
                            logger.info("killing current camera loop")
                            current_mjpeg_loop.cancel()
                            current_mjpeg_loop = None
                    elif current_mjpeg_loop is None:
                        logger.info(
                            f"creating camera loop for beam time {current_beamtime.id}",
                        )
                        current_mjpeg_loop = asyncio.create_task(
                            mjpeg_stream_loop(
                                args.amarcord_url,
                                args.stream_url,
                                args.delay_seconds,
                                current_beamtime.id,
                            ),
                        )
                if previous_failure:
                    logger.info("server is back!")
                    previous_failure = False
            except:
                if not previous_failure:
                    logger.exception(
                        f"couldn't retrieve beam times from {args.amarcord_url}",
                    )
                    previous_failure = True

            await asyncio.sleep(5)


def main() -> None:  # pragma: no cover
    def runner(args: Arguments) -> None:
        asyncio.run(_mjpeg_stream_loop(args))


if __name__ == "__main__":
    main()
