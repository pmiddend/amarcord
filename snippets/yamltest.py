from pathlib import Path

import yaml

with Path("/tmp/test.yaml").open("r") as p:
    print(yaml.load(p))
