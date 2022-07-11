import asyncio
import logging
import re
from dataclasses import dataclass
from time import time
from urllib.parse import unquote

import aiohttp
from pint import UnitRegistry, Quantity

from amarcord.db.asyncdb import AsyncDB
from amarcord.db.event_log_level import EventLogLevel
from amarcord.error_message import ErrorMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Petra3StatusValues:
    milliAmps: Quantity


_SESSION_ID_REGEX = re.compile(r"var sessionId\s*=\s*([0-9]+);")


async def retrieve_petra_initial(
    session: aiohttp.ClientSession, session_id: str
) -> ErrorMessage | None:
    url = f"https://web2c.desy.de/web2cToolkit/Web2c?param=(page)%5b{session_id}%5dpetra/livestatus.xml,0"

    try:
        async with session.get(url) as response:
            if response.status != 200:
                return ErrorMessage(
                    f"Didn't receive a 200 response but {response.status}"
                )
            assert (await response.text()).strip()
            return None
    except Exception as e:
        return ErrorMessage(
            f"Error sending request to {url} to check the status - service offline? Exception message: {e}"
        )


async def retrieve_petra_session_id(
    session: aiohttp.ClientSession,
) -> ErrorMessage | str:
    url = (
        "https://web2c.desy.de/web2cToolkit/Web2c?param=%28open%29petra/livestatus.xml"
    )

    try:
        async with session.get(url) as response:
            if response.status != 200:
                return ErrorMessage(
                    f"Didn't receive a 200 response but {response.status}"
                )
            try:
                html = await response.text()
            except:
                return ErrorMessage(
                    "Error decoding request text, maybe some encoding issue?"
                )
    except Exception as e:
        return ErrorMessage(
            f"Error sending post request to {url} to check the status - service offline? Exception message: {e}"
        )

    match = re.search(_SESSION_ID_REGEX, html)

    if match is None:
        return f"didn't find the sessionId variable value in the returned HTML update message:\n\n{html}"

    return match.group(1)


_MAIN_VALUE_REGEX = re.compile(
    r"<update><id>mainPage_currentValue</id><value>([^<]+)</value>"
)


async def retrieve_petra_status_values(
    ureg: UnitRegistry, session: aiohttp.ClientSession, session_id: str
) -> ErrorMessage | Petra3StatusValues:
    url = (
        f"https://web2c.desy.de/web2cToolkit/Web2c?param=%3Cupdate%3E%3CsessionId%3E{session_id}%3C/sessionId%3E%3CtimeStamp%3E"
        + str(int(time() * 1000))
        + "%3C/timeStamp%3E%3C/update%3E"
    )

    logger.info(f"retrieving url: {url}")

    try:
        async with session.post(url) as response:
            if response.status != 200:
                return ErrorMessage(
                    f"Didn't receive a 200 response but {response.status}"
                )
            try:
                html = await response.text()
            except:
                return ErrorMessage(
                    "Error decoding request text, maybe some encoding issue?"
                )

            unquoted = unquote(html)

            match = re.search(_MAIN_VALUE_REGEX, unquoted)

            if match is None:
                return ErrorMessage(
                    f"didn't find the milli-amperes value in the returned HTML update message:\n\n{unquoted}"
                )

            return Petra3StatusValues(
                milliAmps=float(unquote(match.group(1))) * ureg.mA
            )
    except Exception as e:
        return ErrorMessage(
            f"Error sending post request to {url} to check the status - service offline? Exception message: {e}"
        )


async def _process_result(
    db: AsyncDB,
    beam_down_before: bool | None,
    current_value: ErrorMessage | Petra3StatusValues,
) -> bool | None:
    if isinstance(current_value, ErrorMessage):
        logger.info(f"error retrieving PETRA status values: {current_value}")
        return beam_down_before

    logger.info(f"current amps: {current_value.milliAmps}")
    # Beam is down
    if current_value.milliAmps.m < 10.0:
        # If we _know_ the beam to be down a moment ago, and now it's not - print message.
        if beam_down_before is not None and not beam_down_before:
            async with db.begin() as conn:
                await db.create_event(
                    conn,
                    EventLogLevel.INFO,
                    "🤖 botty",
                    f"Beam dump (beam current {current_value.milliAmps})",
                )
        return True

    # Beam is up now and was down before
    if beam_down_before is not None and beam_down_before:
        async with db.begin() as conn:
            await db.create_event(
                conn,
                EventLogLevel.INFO,
                "🤖 botty",
                f"Beam up again (beam current {current_value.milliAmps})",
            )
        return False

    return False


async def retrieve_petra_session_id_and_initial_values(
    session: aiohttp.ClientSession,
) -> ErrorMessage | str:
    session_id = await retrieve_petra_session_id(session)

    if isinstance(session_id, ErrorMessage):
        return session_id

    logger.info(f"got the session id {session_id}")

    await asyncio.sleep(5.0)

    error = await retrieve_petra_initial(session, session_id)

    if error is not None:
        return error.wrap("error retrieving initial page")

    logger.info("got initial page, waiting a bit until I start receiving updates")

    await asyncio.sleep(10.0)

    return session_id


async def petra3_value_loop(db: AsyncDB, delay_time: float) -> None:
    ureg = UnitRegistry()
    session = aiohttp.ClientSession()

    try:
        beam_down_before: bool | None = None
        logger.info("starting petra3 retrieval")

        while True:
            # Better clear the cookies so we get a clean new session
            session.cookie_jar.clear()
            session_initiated = time()
            session_id = await retrieve_petra_session_id_and_initial_values(session)

            if isinstance(session_id, ErrorMessage):
                logger.error("error retrieving session ID")
                return

            # Retrieve a new session every hour
            while (time() - session_initiated) < 60:
                current_value = await retrieve_petra_status_values(
                    ureg, session, session_id
                )
                beam_down_before = await _process_result(
                    db, beam_down_before, current_value
                )
                await asyncio.sleep(delay_time)
    finally:
        await session.close()
