# import datetime
# from typing import Dict
# from typing import Any
# import sys
# from argparse import ArgumentParser
# import json
# import multiprocessing

# import sqlalchemy as sa
# from rx import of, operators as op
# from rx.scheduler import ThreadPoolScheduler
# import rx

# from extra_data import RunDirectory

# from amarcord.sources.karabo import XFELKaraboBridge
# from amarcord.sources.karabo import XFELKaraboBridgeConfig
# from amarcord.sources.karabo import logger as karabo_logger
# from amarcord.modules.dbcontext import DBContext
# from amarcord.modules.dbcontext import CreationMode

# parser = ArgumentParser(description="Daemon to ingest XFEL data from multiple sources")
# parser.add_argument("--database-url", required=True, help="Database url")
# parser.add_argument("--zeromq-url", required=True, help="ZeroMQ url")

# args = parser.parse_args()


# dbcontext = DBContext(args.database_url)

# table_train = sa.Table(
#     "Train",
#     dbcontext.metadata,
#     sa.Column("run_id", sa.Integer, primary_key=True),
#     sa.Column("train_id", sa.Integer, primary_key=True),
#     sa.Column("created", sa.DateTime, nullable=False),
#     sa.Column("source", sa.String(length=255), nullable=False),
#     sa.Column("metadata", sa.JSON, nullable=False),
# )

# table_train_property = sa.Table(
#     "TrainProperty",
#     dbcontext.metadata,
#     sa.Column("run_id", sa.Integer, primary_key=True),
#     sa.Column("train_id", sa.Integer, primary_key=True),
#     sa.Column("property_key", sa.String(length=255), primary_key=True),
#     sa.Column("property_value", sa.String(length=255)),
# )

# dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)


# def metadata_to_json(metadata: Dict[str, Any]) -> Dict[str, Any]:
#     result: Dict[str, Any] = {}
#     for k, v in metadata.items():
#         if isinstance(v, dict):
#             result[k] = metadata_to_json(v)
#         elif isinstance(v, (bool, int, str)):
#             result[k] = v
#     return result


# tid = 1


# def inject_train_id(metadata: Dict[str, Any]) -> Dict[str, Any]:
#     global tid
#     metadata["ACC_SYS_DOOCS/CTRL/BEAMCONDITIONS"] = {"metadata": {"timestamp.tid": tid}}
#     tid += 1
#     return metadata


# if __name__ == "__main__":

#     def get_train_id(d: Dict[str, Any]) -> str:
#         return d["ACC_SYS_DOOCS/CTRL/BEAMCONDITIONS"]["metadata"]["timestamp.tid"]

#     def create_values(observer, scheduler):
#         karabo = XFELKaraboBridge({"socket_url": args.zeromq_url})
#         try:
#             while True:
#                 observer.on_next(karabo.read_data()[0])
#         except:
#             observer.on_completed()

#     def convert_to_table_row(d: Dict[str, Any]) -> Dict[str, Any]:
#         train_id = get_train_id(d)
#         return {
#             "run_id": 100,
#             "train_id": train_id,
#             "created": datetime.datetime.utcnow(),
#             "source": "online",
#             "metadata": {},
#         }

#     def write_to_db(engine, entries):
#         if not entries:
#             return
#         with engine.connect() as conn:
#             print(f"writing {len(entries)} entry/entries")
#             conn.execute(table_train.insert(), entries)

#     # i = 0
#     print("Beginning reading")

#     source = rx.create(create_values)

#     optimal_thread_count = multiprocessing.cpu_count()
#     pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

#     def print_and_ret(x):
#         print(x)
#         return x

#     # composed = source.pipe(op.map(metadata_to_json), op.buffer(rx.interval(1.0)))
#     composed = source.pipe(
#         op.map(metadata_to_json),
#         # op.map(print_and_ret),
#         # op.map(inject_train_id),
#         # op.buffer_with_time(1.0),
#         # op.map(lambda values: [convert_to_table_row(r) for r in values]),
#         op.map(lambda value: [convert_to_table_row(value)]),
#         op.subscribe_on(pool_scheduler),
#     )
#     composed.subscribe(on_next=lambda v: write_to_db(dbcontext.engine, v))
#     # composed.subscribe(on_next=lambda v: print("values received ", v))
#     print("subscribed")

# with engine.connect() as connection:
#     while True:
#         data, metadata = karabo.read_data()
#         json_data = metadata_to_json(data)
#         # json_metadata = metadata_to_json(metadata)
#         train_id = get_train_id(json_data)
#         if i == 0:
#             print("Read first data frame")
#             # pylint: disable=no-value-for-parameter
#             table_train.insert().values(
#                 run_id=100,
#                 train_id=train_id,
#                 created=datetime.datetime.utcnow(),
#                 source="online",
#                 metadata=json.dumps(json_data),
#             )
#         i += 1
#         if i % 100 == 0:
#             print(f"i={i}, train id {train_id}")

# print("Done!")

# run = RunDirectory("/home/pmidden/data/shared/r0100")
# run.info()  # Show overview info about this data
