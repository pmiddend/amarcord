from typing import Dict
from typing import List
from typing import Any
from typing import Optional
from typing import TypedDict
import logging
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


class XFELMetadataCatalogue:
    def __init__(self, config: XFELMetadataConnectionConfig) -> None:
        self._config = config
        self._connection = MetadataClient(
            client_id=config["user_id"],
            client_secret=config["user_secret"],
            user_email=config["user_email"],
            token_url=config.get(
                "token_url", "https://in.xfel.eu/metadata/oauth/token"
            ),
            refresh_url=config.get(
                "refresh_url", "https://in.xfel.eu/metadata/oauth/token"
            ),
            auth_url=config.get(
                "auth_url", "https://in.xfel.eu/metadata/oauth/authorize"
            ),
            scope=config.get("scope", ""),
            base_api_url=config.get("base_api_url", "https://in.xfel.eu/metadata/api/"),
        )

    def get_proposal_info(self, proposal_id: str) -> Dict[str, Any]:
        return self._connection.get_proposal_info(proposal_id)

    def items(self, proposal_id: str) -> List[Item]:
        logger.info("Getting proposals")
        proposal_info = self._connection.get_proposal_info(proposal_id)
        logger.info("Done getting proposals")
        return [Item("Last Run", proposal_info["data"]["last_run"])]
