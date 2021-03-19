import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TypedDict

from metadata_client import MetadataClient

from amarcord.sources.item import Item

logger = logging.getLogger(__name__)


class XFELMetadataConnectionConfig(TypedDict):
    token_url: Optional[str]
    refresh_url: Optional[str]
    auth_url: Optional[str]
    scope: Optional[str]
    base_api_url: Optional[str]
    user_id: str
    user_secret: str
    user_email: str
    proposal_ids: List[int]


class XFELMetadataCatalogue:
    def __init__(self, config: XFELMetadataConnectionConfig) -> None:
        self._config = config

    def get_proposal_infos(self) -> Dict[int, Any]:
        return {
            pid: self.get_proposal_info(pid) for pid in self._config["proposal_ids"]
        }

    def _connection(self) -> Any:
        return MetadataClient(
            client_id=self._config["user_id"],
            client_secret=self._config["user_secret"],
            user_email=self._config["user_email"],
            token_url=self._config.get(
                "token_url", "https://in.xfel.eu/metadata/oauth/token"
            ),
            refresh_url=self._config.get(
                "refresh_url", "https://in.xfel.eu/metadata/oauth/token"
            ),
            auth_url=self._config.get(
                "auth_url", "https://in.xfel.eu/metadata/oauth/authorize"
            ),
            scope=self._config.get("scope", ""),
            base_api_url=self._config.get(
                "base_api_url", "https://in.xfel.eu/metadata/api/"
            ),
        )

    def get_proposal_info(self, proposal_id: int) -> Dict[str, Any]:
        return self._connection().get_proposal_info(proposal_id)

    def items(self, proposal_id: int) -> List[Item]:
        logger.info("Getting proposals")
        proposal_info = self._connection().get_proposal_info(proposal_id)
        logger.info("Done getting proposals")
        return [Item("Last Run", proposal_info["data"]["last_run"])]
