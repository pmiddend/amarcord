import sys
from pathlib import Path

from amarcord.cli.crystfel_index import crystfel_geometry_hash

print(crystfel_geometry_hash(Path(sys.argv[1])))
