import pickle
import sys
from pathlib import Path

from amarcord.amici.xfel.karabo_bridge_slicer import KaraboBridgeSlicer

with Path(sys.argv[1]).open("rb") as f:
    data, metadata = pickle.load(f)

with Path(sys.argv[2]).open("rb") as f:
    slicer: KaraboBridgeSlicer = pickle.load(f)

slicer.run_definer(data, metadata)
