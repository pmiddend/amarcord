# This module is separate from the normal util.py, because it depends on libraries that are
# only interesting for crystallography.
from typing import Optional

import gemmi


def find_space_group_index_by_name(s: str) -> Optional[int]:
    try:
        return gemmi.find_spacegroup_by_name(s).number
    except:
        return None
