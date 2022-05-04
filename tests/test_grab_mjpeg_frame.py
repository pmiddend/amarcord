from asyncio import StreamReader

import pytest

from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_grab_single_frame_with_reader


def mock_reader(data_to_stream: bytes) -> StreamReader:
    result = StreamReader()
    result.feed_data(data_to_stream)
    result.feed_eof()
    return result


async def test_mjpeg_grab_single_frame_with_reader_successful() -> None:
    image = await mjpeg_grab_single_frame_with_reader(
        mock_reader(
            b"HEADER_LINE_IS_IGNORED\ncontent-type: image/png; boundary=foo\n\nsomeline\n--foo\ncontent-length: 4\n\nabcd"
        )
    )
    assert image == b"abcd"


async def test_mjpeg_grab_single_frame_with_reader_no_boundary() -> None:
    with pytest.raises(Exception):
        await mjpeg_grab_single_frame_with_reader(
            mock_reader(
                # No boundary=bla here, so should fail
                b"HEADER_LINE_IS_IGNORED\ncontent-type: image/png\n\nsomeline\n--foo\ncontent-length: 4\n\nabcd"
            )
        )
