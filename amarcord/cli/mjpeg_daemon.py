import asyncio

from tap import Tap

from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_stream_loop


class Arguments(Tap):
    amarcord_url: str
    stream_url: str
    delay_seconds: float = 5.0
    beamtime_id: int


async def _mjpeg_stream_loop(args: Arguments) -> None:
    await mjpeg_stream_loop(
        args.amarcord_url,
        args.stream_url,
        args.delay_seconds,
        args.beamtime_id,
    )


def main() -> None:
    asyncio.run(_mjpeg_stream_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
