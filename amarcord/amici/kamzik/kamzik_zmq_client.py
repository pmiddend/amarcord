import asyncio
import datetime
import json
import logging
import pickle
from asyncio import FIRST_COMPLETED
from base64 import b64decode
from typing import Any
from typing import Final

import numpy as np
import structlog
import zmq
import zmq.asyncio
from pint import UnitRegistry
from structlog.stdlib import BoundLogger
from zmq.utils.monitor import parse_monitor_message

from amarcord.amici.crystfel.util import ATTRIBUTO_PROTEIN
from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import attributo_types_semantically_equivalent
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.json_schema import parse_schema_type
from amarcord.json_types import JSONDict

_METADATA: Final = "Metadata"
KAMZIK_ATTRIBUTO_GROUP: Final = "kamzik"

# Attributes copied from Kamzik
INSTRUCTION_INIT = b"3"
MSG_JSON = b"J"
MSG_FILE = b"F"
RESPONSE_OK = "1"
MSG_ARRAY = b"A"
MSG_PICKLE = b"P"
TOKEN_ATTRIBUTE = "A!"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = structlog.stdlib.get_logger(__name__)


def JsonKamzikHook(dct: dict[str, Any]) -> Any:
    if "pint-unit-quantity" in dct:
        return UnitRegistry().Quantity.from_tuple(dct["pint-unit-quantity"])
    if "numpy-array" in dct:
        return np.asarray(dct.get("numpy-array"), dtype=dct.get("dtype"))
    if "bytes" in dct:
        return b64decode(dct.get("bytes"))  # type: ignore
    if "kamzik3-device" in dct:
        return {}
    return dct


async def _handle_readout(
    parent_logger: BoundLogger, db: AsyncDB, conn: Connection, token: str, data: Any
) -> None:
    attribute_parts = token.split(".")
    # attribute_id = ".".join(attribute_parts[:-1])
    if attribute_parts[1] == TOKEN_ATTRIBUTE:
        attribute_path = attribute_parts[2:]

        if attribute_path[-2] == _METADATA and attribute_path[-1] == "Value":
            assert isinstance(data, dict)
            parent_logger.info("ingesting new metadata...")
            await ingest_kamzik_metadata(parent_logger, db, conn, data)


def _get_token(device_id_: str, topic: str) -> str:
    return f"{device_id_ if device_id_ is None else device_id_}.{topic}"


async def _monitor_loop(parent_log: BoundLogger, monitor_socket: Any) -> None:
    log = parent_log.bind(loop="monitor loop")
    while True:
        message = parse_monitor_message(await monitor_socket.recv_multipart())  # type: ignore
        log.info(f"received message: {message}")
        if message.get("event") == zmq.EVENT_DISCONNECTED:
            log.info("disconnect")
            break
        if message.get("event") == zmq.EVENT_HANDSHAKE_SUCCEEDED:
            log.info("handshake success")
        else:
            log.info("unimportant event")


async def _subscriber_loop(
    parent_log: BoundLogger, db: AsyncDB, subscriber_socket: Any
) -> None:
    log = parent_log.bind(loop="subscriber loop")
    while True:
        reply = await subscriber_socket.recv_multipart()
        log.info(f"recv: {reply}")
        token, stype = reply[:2]
        data: Any | None = None
        if stype == MSG_JSON:
            data = json.loads(reply[2].decode(), object_hook=JsonKamzikHook)
        elif stype == MSG_ARRAY:
            dtype, shape = reply[2:4]
            reply[4] = np.frombuffer(reply[4], dtype=dtype.decode())  # type: ignore
            data = np.reshape(reply[4], json.loads(shape.decode()))
        elif stype == MSG_PICKLE:
            data = pickle.loads(reply[2])
        if data is not None:
            async with db.begin() as conn:
                await _handle_readout(log, db, conn, token.decode(), data)


async def ingest_kamzik_metadata(
    parent_log: BoundLogger, db: AsyncDB, conn: Connection, metadata: JSONDict
) -> None:
    if metadata == {}:
        parent_log.info("got empty metadata dict")
        return

    run_id = metadata.get("run_id", None)
    assert run_id is not None, f"got no run ID in metadata dict {metadata}"
    assert isinstance(
        run_id, int
    ), f"Run ID is not int, but {run_id} in metadata dict {metadata}"

    run_logger = parent_log.bind(run_id=run_id)

    attributi_schema = metadata.get("attributi-schema", None)
    assert (
        attributi_schema is not None
    ), f"got no attributi-schema in metadata dict {metadata}"
    assert isinstance(
        attributi_schema, dict
    ), f"got no attributi-schema dict in metadata dict {metadata}"

    attributi_values = metadata.get("attributi-values", None)
    assert isinstance(
        attributi_values, dict
    ), f"got no attributi-values in metadata dict {metadata}"

    preexisting_attributi: dict[str, DBAttributo] = {
        t.name: t
        for t in await db.retrieve_attributi(conn, associated_table=AssociatedTable.RUN)
    }

    for attributo_name, attributo_schema in attributi_schema.items():
        decoded_schema = parse_schema_type(attributo_schema)
        attributo_type = schema_to_attributo_type(decoded_schema)

        existing_attributo = preexisting_attributi.get(attributo_name, None)
        if existing_attributo is None:
            await db.create_attributo(
                conn,
                name=attributo_name,
                description="",
                group=KAMZIK_ATTRIBUTO_GROUP,
                type_=attributo_type,
                associated_table=AssociatedTable.RUN,
            )
        else:
            if not attributo_types_semantically_equivalent(
                existing_attributo.attributo_type, attributo_type
            ):
                raise Exception(
                    f"we have a type change, type before: {existing_attributo.attributo_type}, type after: {attributo_type}"
                )

    attributi = await db.retrieve_attributi(conn, associated_table=AssociatedTable.RUN)
    existing_run = await db.retrieve_run(conn, run_id, attributi)
    kamzik_attributi_map = AttributiMap.from_types_and_json(
        attributi,
        chemical_ids=await db.retrieve_chemical_ids(conn),
        raw_attributi=attributi_values,
    )
    attributi_map: AttributiMap
    if existing_run is None:
        attributi_map = kamzik_attributi_map
    else:
        attributi_map = existing_run.attributi
        attributi_map.extend_with_attributi_map(kamzik_attributi_map)
    if existing_run is not None:
        await db.update_run_attributi(conn, run_id, attributi_map)
    else:
        config = await db.retrieve_configuration(conn)
        run_logger.info("creating run in DB")
        await db.create_run(
            conn,
            run_id,
            attributi,
            attributi_map,
            keep_manual_attributes_from_previous_run=config.auto_pilot,
        )
        if config.use_online_crystfel:
            run_logger.info("queueing CrystFEL online indexing job")
            protein_chemical_id = attributi_values.get(ATTRIBUTO_PROTEIN, None)
            if protein_chemical_id is None:
                logger.error("cannot start indexing job, no protein chemical ID found")
                return
            assert isinstance(
                protein_chemical_id, int
            ), f'"protein" value is not int but {protein_chemical_id}'
            chemical = await db.retrieve_chemical(conn, protein_chemical_id, attributi)
            if chemical is None:
                logger.error(
                    f"cannot start indexing job, chemical ID {protein_chemical_id} invalid"
                )
                return
            point_group = chemical.attributi.select_string(AttributoId("point group"))
            cell_description_str = chemical.attributi.select_string(
                AttributoId("cell description")
            )
            cell_description: None | CrystFELCellFile
            if cell_description_str is not None:
                cell_description = parse_cell_description(cell_description_str)
                if cell_description is None:
                    logger.error(
                        f"cannot start indexing job, cell description is invalid: {cell_description_str}"
                    )
                    return
            else:
                cell_description = None

            await db.create_indexing_result(
                conn,
                DBIndexingResultInput(
                    created=datetime.datetime.utcnow(),
                    run_id=run_id,
                    frames=0,
                    hits=0,
                    not_indexed_frames=0,
                    runtime_status=None,
                    point_group=point_group
                    if point_group is not None and point_group.strip()
                    else None,
                    cell_description=cell_description,
                    chemical_id=protein_chemical_id,
                ),
            )


async def kamzik_main_loop(db: AsyncDB, socket_url: str, device_id: str) -> None:
    ctx = zmq.asyncio.Context()  # type: ignore
    socket = ctx.socket(zmq.REQ)  # type: ignore
    socket.setsockopt(zmq.RCVTIMEO, 20000)
    socket.connect(socket_url)
    monitor_socket = socket.get_monitor_socket()

    log = logger.bind(socket_url=socket_url)
    log.info("waiting for connection...")

    while True:
        message = parse_monitor_message(await monitor_socket.recv_multipart())  # type: ignore
        if message.get("event") == zmq.EVENT_CONNECTED:
            break

    async with db.begin() as conn:
        await db.create_event(
            conn, EventLogLevel.INFO, "🤖 kamzik", "Kamzik client (re)started"
        )

    log.info("connected")

    await socket.send_multipart(
        [INSTRUCTION_INIT, device_id.encode(encoding="utf-8")], copy=False
    )

    log.info("waiting for initial package")
    message = await socket.recv_multipart()
    log.info("initial package received")

    status, token, msg_type = message[:3]
    if msg_type == MSG_JSON:
        status, token, response = (
            status.decode(),
            token.decode(),
            message[3].decode(),
        )
    elif msg_type == MSG_FILE:
        status, token, response = (
            status.decode(),
            token.decode(),
            message[3].decode(),
        )
    else:
        raise Exception(f"unknown msg type {msg_type}")

    if status != RESPONSE_OK:
        raise Exception(f"initial package invalid, status: {status}")

    log.info("decoding initial package content")
    (
        attributes,
        attributes_sharing_map,
        _exposed_methods,
        _qualified_name,
        device_proxy,
        device_publisher,
    ) = json.loads(response, object_hook=JsonKamzikHook)

    if device_proxy is not None and device_proxy:
        new_host, new_port = device_proxy
        raise NotImplementedError(f"reconnect to {new_host}:{new_port} now")

    metadata = attributes.get(_METADATA, None)
    if metadata is not None:
        metadata_value = attributes.get("Value", None)
        if metadata_value is not None:
            log.info("initial metadata received, ingesting...")
            async with db.begin() as conn:
                await ingest_kamzik_metadata(log, db, conn, metadata)

    socket.setsockopt(zmq.RCVTIMEO, 5000)

    publisher_host, publisher_port = device_publisher
    subscriber_socket = ctx.socket(zmq.SUB)  # type: ignore
    subscriber_socket.setsockopt(zmq.LINGER, 0)
    subscriber_socket.connect(f"tcp://{publisher_host}:{publisher_port}")

    token = _get_token(device_id, TOKEN_ATTRIBUTE)

    subscriber_socket.setsockopt_string(zmq.SUBSCRIBE, token)

    if attributes_sharing_map:
        log.info(f"got attribute sharing for {attributes_sharing_map}, ignoring")
        # This one is too harsh
        # raise NotImplementedError(f"got attribute sharing for {attributes_sharing_map}")

    log.info("starting main loops")
    await asyncio.wait(
        (
            asyncio.create_task(_monitor_loop(log, monitor_socket)),
            asyncio.create_task(_subscriber_loop(log, db, subscriber_socket)),
        ),
        return_when=FIRST_COMPLETED,
    )
