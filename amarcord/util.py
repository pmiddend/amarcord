from typing import Optional


def str_to_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except:
        return None
