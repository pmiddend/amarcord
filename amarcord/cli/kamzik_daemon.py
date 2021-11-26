import logging
import signal
from pathlib import Path

import kamzik3
import oyaml as yaml
from tap import Tap
from yaml import Loader

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


class Arguments(Tap):
    config_file: str  # Kamzik configuration file path


def mymain(args: Arguments) -> None:

    with Path(args.config_file).open("r") as configFile:
        # this is just magic; it'll create Python classes from the config file
        _config = yaml.load(configFile, Loader=Loader)

    def close_session(_sig, _frame):
        kamzik3.session.stop()

    signal.signal(signal.SIGINT, close_session)


if __name__ == "__main__":
    mymain(Arguments(underscores_to_dashes=True).parse_args())
