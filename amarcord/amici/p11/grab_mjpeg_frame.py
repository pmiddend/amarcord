#!/usr/bin/env python
import asyncio
from asyncio import StreamReader
from urllib.parse import urlparse

import structlog

from amarcord.db.asyncdb import AsyncDB
from amarcord.db.asyncdb import live_stream_image_name

logger = structlog.stdlib.get_logger(__name__)


async def mjpeg_grab_single_frame_with_reader(reader: StreamReader) -> bytes:
    # Ignore HTTP header (first line)
    line = (await reader.readline()).decode("utf-8")

    # Retrieve headers, wait for content type and boundary
    boundary: str | None = None
    header_lines: list[str] = []
    while line.strip() != "":
        header_lines.append(line.strip())
        parts = line.split(":", maxsplit=1)
        if len(parts) > 1 and parts[0].lower() == "content-type":
            # Extract boundary string from content-type
            content_type = parts[1].strip()
            boundary = content_type.split(";")[1].split("=")[1]
        line = (await reader.readline()).decode("utf-8")

    if boundary is None:
        raise Exception(
            "can't find boundary in headers, got the following headers: "
            + ", ".join(header_lines)
        )

    # Seek ahead to the first chunk
    try:
        while line.strip() != "--" + boundary:
            line = (await reader.readline()).decode("utf-8")
    except ValueError:
        raise Exception(f'couldn\'t find content after boundary "{boundary}"')

    # Read in chunk headers
    length: int | None = None
    sub_header_lines: list[str] = []
    while line.strip() != "":
        sub_header_lines.append(line.strip())
        parts = line.split(":", maxsplit=1)
        if len(parts) > 1 and parts[0].lower() == "content-length":
            # Grab chunk length
            length = int(parts[1].strip())
        line = (await reader.readline()).decode("utf-8")

    if length is None:
        raise Exception(
            "didn't receive Content-Length; received the following headers: "
            + ",".join(sub_header_lines)
        )

    image = b""
    while len(image) < length:
        try:
            image = image + await reader.read(length - len(image))
        except ValueError:
            raise Exception(
                f"read {len(image)} byte(s) and got a premature EOF, waiting for {length} byte(s)"
            )
    return image


# Adapted from https://gist.github.com/russss/1143799
async def mjpeg_grab_single_frame(mjpeg_stream_url: str) -> bytes:
    parsed = urlparse(mjpeg_stream_url)

    reader, writer = await asyncio.open_connection(
        parsed.hostname, parsed.port if parsed.port else 80
    )

    get_url = parsed.path.encode("utf-8") + b"?" + parsed.query.encode("utf-8")
    writer.write(b"GET " + get_url + b"\n\n")
    await writer.drain()

    image = await mjpeg_grab_single_frame_with_reader(reader)

    writer.close()
    await writer.wait_closed()

    return image


async def mjpeg_stream_loop(
    db: AsyncDB, mjpeg_stream_url: str, delay: float, beamtime_id: int
) -> None:
    while True:
        try:
            image = await mjpeg_grab_single_frame(mjpeg_stream_url)
        except:
            logger.exception("couldn't grab camera frame, ignoring...")
            await asyncio.sleep(delay)
            continue
        async with db.begin() as conn:
            image_name = live_stream_image_name(beamtime_id)
            existing_file_id = await db.retrieve_file_id_by_name(
                conn, live_stream_image_name(beamtime_id)
            )
            if existing_file_id is None:
                await db.create_file_from_bytes(
                    conn,
                    file_name=image_name,
                    description="",
                    original_path=None,
                    contents=image,
                    deduplicate=False,
                )
            else:
                await db.update_file_from_bytes(
                    conn,
                    existing_file_id,
                    file_name=image_name,
                    description="",
                    original_path=None,
                    contents=image,
                )
        await asyncio.sleep(delay)
