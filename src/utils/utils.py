import json, ast
from typing import Any, Dict, List, Optional

def _is_blank(x) -> bool:
    if x is None: return True
    s = str(x).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}

def parse_json_flexible(value: Optional[str]) -> Any:
    if _is_blank(value):
        return None
    s = str(value)
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        return ast.literal_eval(s)
    except Exception:
        return None

def parse_list(value: Optional[str]) -> List[Any]:
    v = parse_json_flexible(value)
    return v if isinstance(v, list) else []

def parse_dict(value: Optional[str]) -> Dict[str, Any]:
    v = parse_json_flexible(value)
    return v if isinstance(v, dict) else {}

def to_bool(x) -> Optional[bool]:
    if x in (True, False): return x
    if x is None: return None
    xs = str(x).strip().lower()
    if xs in {"true","1","t","yes"}: return True
    if xs in {"false","0","f","no"}: return False
    return None
