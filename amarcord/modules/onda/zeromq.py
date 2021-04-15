import logging
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import NewType
from typing import Optional
from typing import Tuple
from typing import cast

from amarcord.run_id import RunId
from amarcord.train_id import TrainId
from amarcord.util import str_to_int

logger = logging.getLogger(__name__)


@dataclass
class TrainRange:
    train_begin_inclusive: TrainId
    train_end_inclusive: TrainId


HitRate = NewType("HitRate", float)


def _find_run_for_train(
    runs: Dict[RunId, TrainRange], train_id: TrainId
) -> Optional[RunId]:
    return next(
        iter(
            run_id
            for run_id, x in runs.items()
            if x.train_begin_inclusive <= train_id <= x.train_end_inclusive
        ),
        None,
    )


@dataclass(frozen=True)
class OnDAZeroMQData:
    event_id: TrainId
    hit_rate_of_frame: HitRate
    timestamp: float


def validate_onda_zeromq_entry(d: Any) -> Optional[OnDAZeroMQData]:
    if not isinstance(d, dict):
        logger.error(
            "received invalid data from OnDA: not a dictionary but %s", type(d)
        )
        return None

    event_id_str = d.get("event_id", None)
    if event_id_str is None:
        logger.error('received invalid data from OnDA, no "event_id": %s', d)
        return None

    if not isinstance(event_id_str, str):
        logger.error(
            'received invalid data from OnDA, "event_id" is not a string but {%s}: %s',
            type(event_id_str),
            d,
        )
        return None

    event_id = str_to_int(event_id_str)
    if event_id is None:
        logger.error(
            'received invalid data from OnDA, "event_id" is not a proper integer (train ID): %s',
            d,
        )
        return None

    hit_rate_of_frame = d.get("hit_rate_of_frame", None)
    if hit_rate_of_frame is None:
        logger.error('received invalid data from OnDA, no "hit_rate_of_frame": %s', d)
        return None

    timestamp = d.get("timestamp", None)
    if timestamp is None:
        logger.error('received invalid data from OnDA, no "timestamp": %s', d)
        return None

    if not isinstance(timestamp, (int, float)):
        logger.error(
            'received invalid data from OnDA, "timestamp" not number but %s',
            type(timestamp),
        )
        return None

    if not isinstance(hit_rate_of_frame, (int, float)):
        logger.error(
            'received invalid data from OnDA, "hit_rate_of_frame" not a number but %s',
            type(hit_rate_of_frame),
        )
        return None

    return OnDAZeroMQData(TrainId(event_id), HitRate(hit_rate_of_frame), timestamp)


def validate_onda_zeromq_data(d: Any) -> Optional[List[OnDAZeroMQData]]:
    if not isinstance(d, list):
        logger.error("received invalid data from OnDA: not a list but %s", type(d))
        return None

    result: List[OnDAZeroMQData] = []
    for x in d:
        v = validate_onda_zeromq_entry(x)
        if v is None:
            return None
        result.append(v)
    return result


class OnDAZeroMQProcessor:
    def __init__(self) -> None:
        self.current_run: Optional[RunId] = None
        self._number_of_frames: int = 0
        self._sum_of_hit_rates = HitRate(0.0)

    def process_frame(self, frame: OnDAZeroMQData, run_id: RunId) -> Optional[float]:
        # Slight discrepancy here: if we didn't have a run before and now we do, we immediately emit the
        # hit rate we got, since don't have a previous one. If we have a run change, we emit the old hit rate instead.
        if self.current_run is None:
            self.current_run = run_id
            self._number_of_frames = 1
            self._sum_of_hit_rates = frame.hit_rate_of_frame
            return self.hit_rate()
        if self.current_run != run_id:
            self.current_run = run_id
            final_hit_rate = self.hit_rate()
            self._number_of_frames = 1
            self._sum_of_hit_rates = frame.hit_rate_of_frame
            return final_hit_rate
        # And if the hit rate didn't change at all, we just return nothing so we don't emit SQLs all the time.
        self._number_of_frames += 1
        self._sum_of_hit_rates = HitRate(
            frame.hit_rate_of_frame + self._sum_of_hit_rates
        )
        return None

    def hit_rate(self) -> Optional[float]:
        if self.current_run is None or self._number_of_frames == 0:
            return None
        return self._sum_of_hit_rates / self._number_of_frames

    def process_batch(
        self, runs: Dict[RunId, TrainRange], batch: Any
    ) -> Generator[Tuple[float, int], None, None]:
        onda_entries = validate_onda_zeromq_data(batch)

        if onda_entries is None:
            return

        if not onda_entries:
            logger.warning("No entries in OnDA response")
            return

        logger.info("Valid OnDA data received")

        for entry, run_id in (
            (entry, _find_run_for_train(runs, entry.event_id)) for entry in onda_entries
        ):
            if run_id is None:
                logger.warning(
                    "cannot find run ID for train %s, skipping", entry.event_id
                )
                continue
            result = self.process_frame(entry, run_id)
            if result is not None:
                yield result, run_id

        if self.current_run is not None:
            yield cast(float, self.hit_rate()), self.current_run
