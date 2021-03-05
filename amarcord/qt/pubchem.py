from functools import lru_cache

from pubchempy import Compound


@lru_cache
def validate_pubchem_compound(compound_id: int) -> bool:
    try:
        Compound.from_cid(compound_id)
        return True
    except:
        return False
