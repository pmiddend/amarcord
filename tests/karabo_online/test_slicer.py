import datetime
import pickle
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional

import pytest

from amarcord.amici.xfel.karabo_action import KaraboAttributiUpdate
from amarcord.amici.xfel.karabo_action import KaraboRunEnd
from amarcord.amici.xfel.karabo_action import KaraboRunStart
from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_bridge_slicer import KaraboBridgeSlicer
from amarcord.amici.xfel.karabo_cache import KaraboCache
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_online import KaraboValue
from amarcord.amici.xfel.karabo_online import compare_attributi_and_karabo_data
from amarcord.amici.xfel.karabo_online import compare_metadata_trains
from amarcord.amici.xfel.karabo_online import compute_statistics
from amarcord.amici.xfel.karabo_online import generate_train_content
from amarcord.amici.xfel.karabo_online import load_configuration
from amarcord.amici.xfel.karabo_online import parse_configuration
from amarcord.amici.xfel.karabo_stream_keys import KaraboStreamKeys
from amarcord.amici.xfel.karabo_stream_keys import karabo_stream_keys

RUN_CONTROL_KEYS = [
    "runDetails.runId.value",
    "runNumber.value",
    "runDetails.beginAt.value",
    "runDetails.firstTrainId.value",
    "runDetails.length.value",
]

KEY = "newkey"

SOURCE = "newsource"

RUN_CONTROL = "SPB_DAQ_DATA/DM/RUN_CONTROL"


def load_minimal_config() -> Dict[str, Any]:
    config_path = Path(__file__).parent / "config-minimal.yml"
    config = load_configuration(str(config_path))
    return config["Karabo_bridge"]["attributi_definition"]


def load_standard_config() -> Dict[str, Any]:
    config_path = Path(__file__).parent / "config.yml"
    config = load_configuration(str(config_path))
    return config["Karabo_bridge"]["attributi_definition"]


def test_stream_content() -> None:
    assert karabo_stream_keys(
        {"source1": {"key1": 1, "key2": 2}, "source2": {"key3": "foo", "key4": "bar"}},
        {
            "metasource1": {"metakey1": 1, "metakey2": 2},
            "metasource2": {"metakey3": "foo", "metakey4": "bar"},
        },
    ) == KaraboStreamKeys(
        data={"source1": ["key1", "key2"], "source2": ["key3", "key4"]},
        metadata={
            "metasource1": ["metakey1", "metakey2"],
            "metasource2": ["metakey3", "metakey4"],
        },
    )


def test_compare_attributi_and_karabo_data_one_extraneous_attributo_with_ignore() -> None:
    """
    This tests the comparison function which checks if there is unexpected data in the Karabo stream.

    Assume a standard config, but add another source with two keys. Specify one key to be ignored. Nothing should be
    returned, as the new key should be properly ignored.
    """
    attributi, expected_attributi = parse_configuration(load_minimal_config())

    expected_attributi["newsource"] = {}
    expected_attributi["newsource"]["newkey"] = {
        "attributo": create_random_attributo(),
        "group": "newgroup",
    }
    result = compare_attributi_and_karabo_data(
        expected_attributi,
        KaraboStreamKeys(
            data={
                RUN_CONTROL: RUN_CONTROL_KEYS,
                "newsource": ["newkey", "newkey2"],
            },
            metadata={},
        ),
        ignore_entry={"newsource": frozenset({"newkey2"})},
    )

    assert not result


def create_random_attributo(
    filling_value: Optional[KaraboValue] = None,
    action: KaraboAttributoAction = KaraboAttributoAction.STORE_LAST,
) -> KaraboAttributo:
    return KaraboAttributo(
        "newidentifier",
        "newsource",
        "newkey",
        "description",
        "int",
        store=True,
        action=action,
        action_axis=None,
        unit="",
        filling_value=filling_value,
        value=None,
        role=None,
    )


def test_cache_train_wrong_type() -> None:
    """
    When caching values, we check their type against what's configured. If the check fails, we don't accept the value.
    """
    expected: KaraboExpectedAttributi = {
        "newsource": {
            "newkey": {
                "group": "mygroup",
                "attributo": create_random_attributo(),
            },
        }
    }
    cache = KaraboCache(expected)
    cache.cache_train(
        data={
            # Note: it's a string, the attributo above is an int
            "newsource": {"newkey": "foo"},
        },
        expected_attributi=expected,
        bridge_content=KaraboStreamKeys(data={"newsource": ["newkey"]}, metadata={}),
    )
    assert cache.cached_events == 1
    assert cache.content["newsource"]["newkey"] == []


def test_cache_train_non_initial_key_is_ignored() -> None:
    """
    This checks if the train cache is populated for a single key. The cache function should only cache
    the values the bridge initially sent, so we only pass in one of the two keys here and check that the
    other wasn't cached.
    """
    expected: KaraboExpectedAttributi = {
        "newsource": {
            "newkey": {
                "group": "mygroup",
                "attributo": create_random_attributo(),
            },
        }
    }
    cache = KaraboCache(expected)
    cache.cache_train(
        data={
            "newsource": {"newkey": 1, "newkey2": "str"},
        },
        expected_attributi=expected,
        bridge_content=KaraboStreamKeys(data={"newsource": ["newkey"]}, metadata={}),
    )
    assert cache.cached_events == 1
    assert "newkey" in cache.content["newsource"]
    assert "newkey2" not in cache.content["newsource"]
    assert cache.content["newsource"]["newkey"] == 1


def test_cache_train_filling_value_needed() -> None:
    """
    In case we want to cache a train property but got no value, we can enter a filling value, which is
    done here and tested.
    """
    expected: KaraboExpectedAttributi = {
        "newsource": {
            "newkey": {
                "group": "mygroup",
                "attributo": create_random_attributo(filling_value=3),
            },
        }
    }
    cache = KaraboCache(expected)
    # note that "data" here doesn't contain all expected attributi, so we expect the filling value
    # to be used for the cache
    cache.cache_train(
        data={
            "newsource": {},
        },
        expected_attributi=expected,
        bridge_content=KaraboStreamKeys(data={"newsource": ["newkey"]}, metadata={}),
    )
    assert cache.cached_events == 1
    assert "newkey" in cache.content["newsource"]
    assert cache.content["newsource"]["newkey"] == 3


def test_cache_train_append() -> None:
    """
    Here we test the "compute arithmetic mean" function, which should store values in the cache rather than
    overwrite the previous one.
    """
    expected: KaraboExpectedAttributi = {
        "newsource": {
            "newkey": {
                "group": "mygroup",
                "attributo": create_random_attributo(
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN
                ),
            },
        }
    }
    cache = KaraboCache(expected)
    # note that "data" here doesn't contain all expected attributi, so we expect the filling value
    # to be used for the cache
    cache.cache_train(
        data={
            "newsource": {"newkey": 3},
        },
        expected_attributi=expected,
        bridge_content=KaraboStreamKeys(data={"newsource": ["newkey"]}, metadata={}),
    )
    assert cache.cached_events == 1
    assert "newkey" in cache.content["newsource"]
    # Note the array here
    assert cache.content["newsource"]["newkey"] == [3]


def test_compare_attributi_and_karabo_data_ignore_whole_source() -> None:
    """
    Here we completely ignore the source and pass extra attributo from there. Shouldn't matter.
    """
    attributi, expected_attributi = parse_configuration(load_minimal_config())

    result = compare_attributi_and_karabo_data(
        expected_attributi,
        KaraboStreamKeys(
            data={
                RUN_CONTROL: RUN_CONTROL_KEYS,
                "newsource": ["newkey", "newkey2"],
            },
            metadata={},
        ),
        ignore_entry={"newsource": frozenset({"IGNOREALL"})},
    )

    # Only one key will be shown as extraneous
    print(result)
    assert not result


def test_compare_attributi_and_karabo_data_one_extraneous_attributo() -> None:
    """
    Here we pass in exactly one extra attributo ("newkey") an expect a message about that from the comparison function.
    """
    attributi, expected_attributi = parse_configuration(load_minimal_config())

    expected_attributi["newsource"] = {}
    expected_attributi["newsource"]["newkey"] = {
        "attributo": KaraboAttributo(
            "newidentifier",
            "newsource",
            "newkey",
            "description",
            "int",
            store=True,
            action=KaraboAttributoAction.STORE_LAST,
            action_axis=None,
            unit="",
            filling_value=None,
            value=None,
            role=None,
        ),
        "group": "newgroup",
    }
    result = compare_attributi_and_karabo_data(
        expected_attributi,
        KaraboStreamKeys(
            data={
                RUN_CONTROL: RUN_CONTROL_KEYS,
                "newsource": ["newkey", "newkey2"],
            },
            metadata={},
        ),
        ignore_entry={},
    )

    # Only one key will be shown as extraneous
    print(result)
    assert len(result) == 1


def test_compare_attributi_and_karabo_data_one_extraneous_source() -> None:
    """
    Here we have one extraneous source we didn't specify in the expected attributi. It should be shown as such.
    """
    attributi, expected_attributi = parse_configuration(load_minimal_config())

    result = compare_attributi_and_karabo_data(
        expected_attributi,
        KaraboStreamKeys(
            data={
                RUN_CONTROL: RUN_CONTROL_KEYS,
                "newsource": ["newvalue1", "newvalue2"],
            },
            metadata={},
        ),
        ignore_entry={},
    )

    # The whole source will be shown as "not requested"
    assert len(result) == 1


def test_compare_attributi_and_karabo_data_one_attributo_not_available() -> None:
    """
    We specify one expected attributo and then assume we get a message if it's missing in the stream.
    """
    attributi, expected_attributi = parse_configuration(load_minimal_config())

    expected_attributi["newsource"] = {}
    expected_attributi["newsource"]["newkey"] = {
        "attributo": create_random_attributo(),
        "group": "newgroup",
    }

    result = compare_attributi_and_karabo_data(
        expected_attributi,
        KaraboStreamKeys(
            data={RUN_CONTROL: RUN_CONTROL_KEYS},
            metadata={},
        ),
        ignore_entry={},
    )

    # We will only be missing our custom value
    assert len(result) == 1


def test_generate_train_content() -> None:
    """
    This tests the basic function of the train config parser (which contains the most important values for the train)
    """
    attributi, _ = parse_configuration(load_standard_config())

    result = generate_train_content(
        attributi,
        {
            RUN_CONTROL: {
                "runDetails.runId.value": 1,
                "runNumber.value": 2,
                "proposalNumber.value": 2,
                "runDetails.beginAt.value": "20210413T175018.106324Z",
                "runDetails.firstTrainId.value": 3,
                "runDetails.length.value": 20,
            }
        },
    )

    assert result is not None
    assert result["number"].value == 2
    assert result["index"].value == 1
    assert result["timestamp_UTC_initial"].value == "20210413T175018.106324Z"
    assert result["train_index_initial"].value == 3
    assert result["trains_in_run"].value == 20


def test_compare_metadata_trains() -> None:
    """
    This tests the function that checks if all train IDs in the metadata are "in sync"
    with positive and negative examples.
    """
    assert (
        compare_metadata_trains(
            {
                "source1": {"timestamp.tid": 3},
                "source2": {"timestamp.tid": 3},
            }
        )
        == 3
    )

    with pytest.raises(ValueError):
        compare_metadata_trains(
            {
                "source1": {"timestamp.tid": 3},
                "source2": {"timestamp.tid": 4},
            }
        )

    with pytest.raises(ValueError):
        assert compare_metadata_trains({})


def test_start_with_run_in_progress() -> None:
    """
    Load the standard config and a pickle file (which will be in the middle of a run) and simulate that the run
    just started. Assume we get a "Run started" message back.
    """
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        train_id = compare_metadata_trains(metadata)
        data[RUN_CONTROL]["proposalNumber.value"] = 1

    # We set the run to have "just started" (train ID equal to the train ID in the data frame)
    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id
    data[RUN_CONTROL]["runDetails.length.value"] = 0
    run_start_time = datetime.datetime(1987, 8, 21, 15, 0, 0, 0)
    data[RUN_CONTROL]["runDetails.beginAt.value"] = "19870821T150000.0Z"
    data[RUN_CONTROL]["runNumber.value"] = 1337

    # The run just started, so we expect it to be returned here
    result = karabo_data.run_definer(data, metadata)
    assert len(result) == 1
    assert isinstance(result[0], KaraboRunStart)
    assert result[0].run_id == 1337


def create_standard_slicer() -> KaraboBridgeSlicer:
    config_path = Path(__file__).parent / "config.yml"
    config = load_configuration(str(config_path))
    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])
    return karabo_data


def create_minimal_slicer() -> KaraboBridgeSlicer:
    config_path = Path(__file__).parent / "config-minimal.yml"
    config = load_configuration(str(config_path))
    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])
    return karabo_data


def test_start_with_run_in_progress_for_too_long() -> None:
    """
    Here we test the feature that a run is ignored if it started too far back (so the statistics won't
    make sense anymore).
    """
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        data[RUN_CONTROL]["proposalNumber.value"] = 1
        train_id = compare_metadata_trains(metadata)

    # The run didn't "just" start, but 100 trains ago, so we're out of luck on this one
    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id - 100
    data[RUN_CONTROL]["runDetails.length.value"] = 0
    data[RUN_CONTROL]["runNumber.value"] = 1337

    # Assume no change
    result = karabo_data.run_definer(data, metadata, train_cache_size=5)
    assert not result


def test_start_with_run_in_progress_but_not_too_long() -> None:
    """
    This explicitly tests the feature that a run is _not_ ignored if it started in a certain window.
    """
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        train_id = compare_metadata_trains(metadata)
        data[RUN_CONTROL]["proposalNumber.value"] = 1

    # The run just started, this frame. We expect an event here
    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id
    data[RUN_CONTROL]["runDetails.length.value"] = 0
    data[RUN_CONTROL]["runNumber.value"] = 1337

    # Assume no change
    result = karabo_data.run_definer(data, metadata, train_cache_size=5)
    assert len(result) == 1
    assert isinstance(result[0], KaraboRunStart)


def test_start_with_run_then_update_it() -> None:
    """
    Here we simulate the start of a run and an update afterwards
    """
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        train_id = compare_metadata_trains(metadata)
        data[RUN_CONTROL]["proposalNumber.value"] = 1

    # The run just started, this frame. We expect an event here
    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id
    data[RUN_CONTROL]["runDetails.length.value"] = 0
    data[RUN_CONTROL]["runNumber.value"] = 1337

    # Assume start
    result = karabo_data.run_definer(data, metadata, train_cache_size=5)
    assert len(result) == 1
    assert isinstance(result[0], KaraboRunStart)

    result = karabo_data.run_definer(
        data, metadata, train_cache_size=5, averaging_interval=0
    )
    assert len(result) == 1
    assert isinstance(result[0], KaraboAttributiUpdate)


def test_start_with_run_then_update_it_with_update_window() -> None:
    """
    In this test, we assume we get a run start and then introduce another train, but we don't expect to get
    an update message because the statistics are not "done" yet (averaging didn't have enough time).
    """
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        train_id = compare_metadata_trains(metadata)
        data[RUN_CONTROL]["proposalNumber.value"] = 1

    # The run just started, this frame. We expect an event here
    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id
    data[RUN_CONTROL]["runDetails.length.value"] = 0
    data[RUN_CONTROL]["runNumber.value"] = 1337

    # Assume start
    result = karabo_data.run_definer(data, metadata, train_cache_size=5)
    assert len(result) == 1
    assert isinstance(result[0], KaraboRunStart)
    assert result[0].proposal_id == 1

    # If averaging interval is set, we don't except an update yet
    result = karabo_data.run_definer(
        data, metadata, train_cache_size=5, averaging_interval=5
    )
    assert not result


def test_run_close() -> None:
    """
    We create a new run, expect it to be created, and then close it.
    """
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        data[RUN_CONTROL]["proposalNumber.value"] = 1
        metadata = dataset_content["metadata"]
        train_id = compare_metadata_trains(metadata)

    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id
    # Run is not over yet
    data[RUN_CONTROL]["runDetails.length.value"] = 0

    # Assume we get a "new run" message since the train ID adds up
    result = karabo_data.run_definer(data, metadata, averaging_interval=1)
    assert result

    # Now we close the run
    data[RUN_CONTROL]["runDetails.length.value"] = 10

    # Expect a run end message
    result = karabo_data.run_definer(data, metadata, averaging_interval=1)
    assert len(result) == 1
    assert isinstance(result[0], KaraboRunEnd)


def test_compute_statistics_with_store_last() -> None:
    karabo_data = create_minimal_slicer()

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        data[RUN_CONTROL]["proposalNumber.value"] = 1

    # Assume no change
    result = karabo_data.run_definer(data, metadata, averaging_interval=1)
    assert not result


def test_receive_ended_run() -> None:
    """
    Here we test the scenario that the daemon is started and receives a run that's already over.

    In this case, we expect no message, since we cannot generate correct averages and also don't get
    the final results from Karabo.
    """
    config_path = Path(__file__).parent / "config.yml"

    config = load_configuration(str(config_path))

    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    # Load a basic file (could be middle of a run)
    with (Path(__file__).parent / "events" / "1035758364.pickle").open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        train_id = compare_metadata_trains(metadata)
        data[RUN_CONTROL]["proposalNumber.value"] = 1

    # The run has ended (see the length), we don't know it yet, so we expect no message in the end
    data[RUN_CONTROL]["runDetails.firstTrainId.value"] = train_id
    data[RUN_CONTROL]["runDetails.length.value"] = 10
    data[RUN_CONTROL]["runNumber.value"] = 1337

    # Assume start
    result = karabo_data.run_definer(data, metadata, train_cache_size=5)
    assert not result


def test_intermittently_missing_source_without_filling_value_and_store_last() -> None:
    """
    Sources might disappear during a run. Here we test that feature, with an attributo with a filling value and without
    one.
    """
    attributi = create_minimal_attributi_definition()
    attributi["newgroup"] = {
        "testattributo": {
            "key": "testattributo",
            "type": "decimal",
            "source": "newsource",
            "action": "store_last",
        }
    }

    karabo_data = KaraboBridgeSlicer(
        attributi_definition=attributi,
        ignore_entry={},
    )

    # initial ingest
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            "newsource": {"testattributo": 1},
        },
        metadata={"device1": {"timestamp.tid": 1}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # no stats yet
    assert result[0].attributi["newgroup"]["testattributo"].value is None

    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            # different value so we have a nicer mean than "constant value"
            "newsource": {"testattributo": 2},
        },
        metadata={"device1": {"timestamp.tid": 2}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # We are already averaging!
    assert result[0].attributi["newgroup"]["testattributo"].value == 2

    # third train, source is missing
    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
        },
        metadata={"device1": {"timestamp.tid": 3}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # We're missing a value, but we have keep last, so the last value is fine here
    assert result[0].attributi["newgroup"]["testattributo"].value == 2


def test_intermittently_missing_source_with_filling_value_and_store_last() -> None:
    """
    Sources might disappear during a run. Here we test that feature, with an attributo with a filling value and without
    one.
    """
    attributi = create_minimal_attributi_definition()
    attributi["newgroup"] = {
        "testattributo": {
            "key": "testattributo",
            "type": "decimal",
            "source": "newsource",
            "filling_value": 100,
            "action": "store_last",
        }
    }

    karabo_data = KaraboBridgeSlicer(
        attributi_definition=attributi,
        ignore_entry={},
    )

    # initial ingest
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            "newsource": {"testattributo": 1},
        },
        metadata={"device1": {"timestamp.tid": 1}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # no stats yet
    assert result[0].attributi["newgroup"]["testattributo"].value is None

    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            # different value so we have a nicer mean than "constant value"
            "newsource": {"testattributo": 2},
        },
        metadata={"device1": {"timestamp.tid": 2}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # We are already averaging!
    assert result[0].attributi["newgroup"]["testattributo"].value == 2

    # third train, source is missing
    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
        },
        metadata={"device1": {"timestamp.tid": 3}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # The thing is missing so we use 100 as the filling value
    assert result[0].attributi["newgroup"]["testattributo"].value == 100


def test_intermittently_missing_source_without_filling_value_averaging() -> None:
    """
    Sources might disappear during a run. Here we test that feature, with an attributo with a filling value and without
    one.
    """
    attributi = create_minimal_attributi_definition()
    attributi["newgroup"] = {
        "testattributo": {
            "key": "testattributo",
            "type": "decimal",
            "source": "newsource",
            "action": "compute_arithmetic_mean",
        }
    }

    karabo_data = KaraboBridgeSlicer(
        attributi_definition=attributi,
        ignore_entry={},
    )

    # initial ingest
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            "newsource": {"testattributo": 1},
        },
        metadata={"device1": {"timestamp.tid": 1}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # no stats yet
    assert result[0].attributi["newgroup"]["testattributo"].value is None

    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            # different value so we have a nicer mean than "constant value"
            "newsource": {"testattributo": 2},
        },
        metadata={"device1": {"timestamp.tid": 2}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # We are already averaging!
    assert result[0].attributi["newgroup"]["testattributo"].value == 1.5

    # third train, source is missing
    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
        },
        metadata={"device1": {"timestamp.tid": 3}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # Average will still be 1.5, since "None" will be used as filling value
    assert result[0].attributi["newgroup"]["testattributo"].value == 1.5


def test_intermittently_missing_source_with_filling_value() -> None:
    """
    Sources might disappear during a run. Here we test that feature, with an attributo with a filling value and without
    one.
    """
    attributi = create_minimal_attributi_definition()
    attributi["newgroup"] = {
        "testattributo": {
            "key": "testattributo",
            "type": "decimal",
            "filling_value": 0,
            "source": "newsource",
            "action": "compute_arithmetic_mean",
        }
    }

    karabo_data = KaraboBridgeSlicer(
        attributi_definition=attributi,
        ignore_entry={},
    )

    # initial ingest
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            "newsource": {"testattributo": 1},
        },
        metadata={"device1": {"timestamp.tid": 1}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # no stats yet
    assert result[0].attributi["newgroup"]["testattributo"].value is None

    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
            # different value so we have a nicer mean than "constant value"
            "newsource": {"testattributo": 2},
        },
        metadata={"device1": {"timestamp.tid": 2}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # We are already averaging!
    assert result[0].attributi["newgroup"]["testattributo"].value == 1.5

    # third train, source is missing
    # second train, now we have a value!
    result = karabo_data.run_definer(
        data={
            "run": {
                "run_idx": 1,
                "run_number": 1,
                "timestamp_initial": "19870821T150000.0Z",
                "initial": 1,
                "trains_in_run": 0,
                "proposal_id": 1,
            },
        },
        metadata={"device1": {"timestamp.tid": 3}},
        averaging_interval=1,
    )

    assert len(result) == 1
    # Average will still be 1.5
    assert result[0].attributi["newgroup"]["testattributo"].value == 1.5


def create_minimal_attributi_definition() -> Dict[str, Any]:
    return {
        "run": {
            "source": "run",
            "index": {
                "key": "run_idx",
                "type": "int",
                "role": "run_id",
            },
            "number": {
                "key": "run_number",
                "type": "int",
                "role": "run_number",
            },
            "timestamp_UTC_initial": {
                "key": "timestamp_initial",
                "type": "datetime",
            },
            "train_index_initial": {
                "key": "initial",
                "type": "int",
            },
            "proposal_id": {
                "key": "proposal_id",
                "type": "int",
                "role": "proposal_id",
            },
            "trains_in_run": {
                "key": "trains_in_run",
                "type": "int",
            },
        }
    }


def test_compute_statistics_arithmetic_mean() -> None:
    """
    Here we test the compute_statistics function, specifically computing the arithmetic mean.
    """
    attributo = create_random_attributo(
        action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN
    )
    expected: KaraboExpectedAttributi = {
        SOURCE: {
            KEY: {
                "group": "run",
                "attributo": attributo,
            },
        }
    }
    attributi = {"run": {"newidentifier": attributo}}
    cache = KaraboCache(expected)
    cache.content[SOURCE][KEY] = [3, 4, 5]
    compute_statistics(cache.content, expected, attributi, current_run=1)
    assert attributo.value == 4


def test_compute_statistics_arithmetic_mean_ignore_filling_values() -> None:
    """
    Here we test the compute_statistics function, specifically computing the arithmetic mean.
    """
    attributo = create_random_attributo(
        filling_value=0, action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN
    )
    expected: KaraboExpectedAttributi = {
        SOURCE: {
            KEY: {
                "group": "run",
                "attributo": attributo,
            },
        }
    }
    attributi = {"run": {"newidentifier": attributo}}
    cache = KaraboCache(expected)
    cache.content[SOURCE][KEY] = [0, 3, 0, 4, 0, 5]
    compute_statistics(cache.content, expected, attributi, current_run=1)
    assert attributo.value == 4


def test_compute_statistics_check_if_constant() -> None:
    attributo = create_random_attributo(action=KaraboAttributoAction.CHECK_IF_CONSTANT)
    expected: KaraboExpectedAttributi = {
        SOURCE: {
            KEY: {
                "group": "run",
                "attributo": attributo,
            },
        }
    }
    attributi = {"run": {"newidentifier": attributo}}
    cache = KaraboCache(expected)
    cache.content[SOURCE][KEY] = [3, 4, 5]
    compute_statistics(cache.content, expected, attributi, current_run=1)
    # Take the first one if not constant
    assert attributo.value == 3


def test_ingest_all_pickles() -> None:
    karabo_data = create_standard_slicer()

    # Load a basic file (could be middle of a run)
    for f in (Path(__file__).parent / "events").glob("*.pickle"):
        with f.open("rb") as handle:
            dataset_content = pickle.load(handle)
            data = dataset_content["data"]
            metadata = dataset_content["metadata"]
            data[RUN_CONTROL]["proposalNumber.value"] = 1
            _train_id = compare_metadata_trains(metadata)

            # The run just started, so we expect it to be returned here
            result = karabo_data.run_definer(data, metadata)

            print(result)
