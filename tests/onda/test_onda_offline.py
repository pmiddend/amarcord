from amarcord.modules.onda.zeromq import HitRate
from amarcord.modules.onda.zeromq import OnDAZeroMQData
from amarcord.modules.onda.zeromq import OnDAZeroMQProcessor
from amarcord.modules.onda.zeromq import TrainRange
from amarcord.run_id import RunId
from amarcord.train_id import TrainId


def test_onda_processor_first_frame() -> None:
    """Process a single incoming frame and assume this immediately triggers the signal for a run change"""
    p = OnDAZeroMQProcessor()

    result = p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(0.5), 0), RunId(1))

    assert result == HitRate(0.5)


def test_onda_processor_subsequent_frame() -> None:
    """Check if processing two frames triggers no change on the second frame"""
    p = OnDAZeroMQProcessor()

    result_1 = p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(0.5), 0), RunId(1))
    result_2 = p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(0.8), 0), RunId(1))

    assert result_1 is not None
    assert result_2 is None


def test_onda_processor_run_change() -> None:
    """Check if our run change signal is emitted"""
    p = OnDAZeroMQProcessor()

    result_1 = p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(0.5), 0), RunId(1))
    # Note, changed run!
    result_2 = p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(0.8), 0), RunId(2))

    assert result_1 == HitRate(0.5)
    # Intentional: the old hit rate is returned
    assert result_2 == HitRate(0.5)

    assert p.hit_rate() == HitRate(0.8)


def test_onda_processor_hit_rate_is_averaged() -> None:
    """Check if we're really averaging"""
    p = OnDAZeroMQProcessor()

    p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(0.5), 0), RunId(1))
    p.process_frame(OnDAZeroMQData(TrainId(1), HitRate(1.0), 0), RunId(1))

    assert p.hit_rate() == HitRate(0.75)


def test_process_empty_batch() -> None:
    """Check if ingesting an empty batch is fine and doesn't throw or anything"""
    p = OnDAZeroMQProcessor()

    # List because we want to consume everything
    list(p.process_batch({}, []))


def test_process_singleton_batch() -> None:
    """Check if ingesting a batch with a single element works"""
    p = OnDAZeroMQProcessor()

    results = list(
        p.process_batch(
            {RunId(1): TrainRange(TrainId(1), TrainId(999))},
            [{"event_id": "1", "hit_rate_of_frame": 0.5, "timestamp": 0}],
        )
    )

    # This is the special case of the very first frame which gets duplicated. I could fix this, but it's too much of
    # a hassle, really.
    assert results == [(HitRate(0.5), RunId(1)), (HitRate(0.5), RunId(1))]


def test_process_singleton_batch() -> None:
    """Check if we're really averaging"""
    p = OnDAZeroMQProcessor()

    results = list(
        p.process_batch(
            {RunId(1): TrainRange(TrainId(1), TrainId(999))},
            [{"event_id": "1", "hit_rate_of_frame": 0.5, "timestamp": 0}],
        )
    )

    # This is the special case of the very first frame which gets duplicated. I could fix this, but it's too much of
    # a hassle, really.
    assert results == [(HitRate(0.5), RunId(1)), (HitRate(0.5), RunId(1))]


def test_process_two_items_in_batch() -> None:
    """Check if ingesting a batch two elements works"""
    p = OnDAZeroMQProcessor()

    results = list(
        p.process_batch(
            {RunId(1): TrainRange(TrainId(1), TrainId(999))},
            [
                {"event_id": "1", "hit_rate_of_frame": 0.5, "timestamp": 0},
                {"event_id": "2", "hit_rate_of_frame": 1.0, "timestamp": 0},
            ],
        )
    )

    # Special case for the first frame, then the averaged hit rate for the batch.
    assert results == [(HitRate(0.5), RunId(1)), (HitRate(0.75), RunId(1))]


def test_process_batch_with_dangling_run_id() -> None:
    """Check if it's okay for a batch to have a run change to a run where we don't know the trains yet."""
    p = OnDAZeroMQProcessor()

    results = list(
        p.process_batch(
            {RunId(1): TrainRange(TrainId(1), TrainId(999))},
            [
                {"event_id": "1", "hit_rate_of_frame": 0.5, "timestamp": 0},
                {"event_id": "10000", "hit_rate_of_frame": 1.0, "timestamp": 0},
            ],
        )
    )

    assert results == [(HitRate(0.5), RunId(1)), (HitRate(0.5), RunId(1))]
