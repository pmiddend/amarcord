from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List

from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_source import KaraboSource


@dataclass(frozen=True)
class KaraboStreamKeys:
    data: Dict[KaraboSource, List[str]]
    metadata: Dict[KaraboSource, List[str]]


def karabo_stream_keys(
    data: KaraboData,
    metadata: Dict[str, Any],
) -> KaraboStreamKeys:
    """Navigate the stream from the Karabo bridge

    Args:
        data (Dict[str, Any]): The Karabo bridge stream data
        metadata (Dict[str, Any]): The Karabo bridge stream metadata

    Returns:
        Dict[str, List[str]]: The stream content
    """

    def extractor(stream: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
        container: Dict[str, List[str]] = {}

        for si, si_content in stream.items():
            container[si] = []

            for ki in si_content.keys():
                container[si].append(ki)

        return container

    return KaraboStreamKeys(data=extractor(data), metadata=extractor(metadata))
