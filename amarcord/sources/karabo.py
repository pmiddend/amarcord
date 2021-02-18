from typing import Dict
from typing import List
from typing import Any
from typing import Optional
from typing import TypedDict
from typing import Tuple
import logging
from karabo_bridge import Client
from amarcord.sources.item import Item

logger = logging.getLogger(__name__)


class XFELKaraboBridgeConfig(TypedDict):
    soFcket_url: Optional[str]


class XFELKaraboBridge:
    def __init__(self, config: XFELKaraboBridgeConfig) -> None:
        self._config = config
        self._client = Client(config.get("socket_url", "tcp://localhost:4545"))

    def read_data(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # pylint: disable=not-callable
        return self._client.next()

    def items(self) -> List[Item]:
        data, metadata = self.read_data()
        logger.info("got some items: %s", metadata.keys())
        return [
            Item(
                "timestamp",
                metadata["SPB_DET_AGIPD1M-1/CAL/APPEND_CORRECTED"]["timestamp"],
            ),
            Item(
                "metadata",
                metadata["SPB_DET_AGIPD1M-1/CAL/APPEND_CORRECTED"],
            ),
            Item(
                "data",
                data["SPB_DET_AGIPD1M-1/CAL/APPEND_CORRECTED"],
            ),
        ]
