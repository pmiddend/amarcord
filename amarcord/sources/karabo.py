from typing import Dict
from typing import List
from typing import Any
from typing import Tuple
from dataclasses import dataclass
from karabo_bridge import Client
from amarcord.sources.item import Item


@dataclass(frozen=True)
class XFELKaraboBridgeConfig:
    socket_url = "tcp://localhost:4545"


class XFELKaraboBridge:
    def __init__(self, config: XFELKaraboBridgeConfig) -> None:
        self._config = config
        self._client = Client(config.socket_url)

    def read_data(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # pylint: disable=not-callable
        return self._client.next()

    def items(self) -> List[Item]:
        _data, metadata = self.read_data()
        print(metadata.keys())
        return []
