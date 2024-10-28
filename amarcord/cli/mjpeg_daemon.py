import asyncio
import datetime

import aiohttp
import structlog
from tap import Tap

from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_stream_loop
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.web.json_models import JsonReadBeamtime

logger = structlog.stdlib.get_logger(__name__)


class Arguments(Tap):
    amarcord_url: str
    stream_url: str
    delay_seconds: float = 5.0


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

                    now = datetime.datetime.now(datetime.timezone.utc)
                    current_beamtime = None
                    for beamtime in beamtimes.beamtimes:
                        if (
                            datetime_from_attributo_int(beamtime.start)
                            < now
                            < datetime_from_attributo_int(beamtime.end)
                        ):
                            current_beamtime = beamtime

                    if current_beamtime is None:
                        if current_mjpeg_loop is not None:
                            logger.info("killing current camera loop")
                            current_mjpeg_loop.cancel()
                            current_mjpeg_loop = None
                    else:
                        if current_mjpeg_loop is None:
                            logger.info(
                                f"creating camera loop for beam time {current_beamtime.id}"
                            )
                            current_mjpeg_loop = asyncio.create_task(
                                mjpeg_stream_loop(
                                    args.amarcord_url,
                                    args.stream_url,
                                    args.delay_seconds,
                                    current_beamtime.id,
                                )
                            )
                if previous_failure:
                    logger.info("server is back!")
                    previous_failure = False
            except:
                if not previous_failure:
                    logger.exception(
                        f"couldn't retrieve beam times from {args.amarcord_url}"
                    )
                    previous_failure = True

            await asyncio.sleep(5)


def main() -> None:
    asyncio.run(_mjpeg_stream_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
