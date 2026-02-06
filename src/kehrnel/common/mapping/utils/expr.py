# src/mapper/utils/expr.py
import re
def evaluate(expr: str, *, row=None, vars=None) -> bool:
    row = row or {}; vars = vars or {}
    # operators: ==, !=, ~=, !~= ; support ${var} if needed later
    m = re.match(r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ.]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$', expr)
    if not m:
        # allow xpath() style vars-based checks: e.g., "xpath('//cda:id') != ''"
        if "xpath" in vars and expr.startswith("xpath("):
            try:
                inside = expr[len("xpath("):-1]
                val = vars["xpath"](inside)
                return bool(val)
            except: return False
        return True
    key, op, rhs = m.groups()
    val = str((row.get(key) if isinstance(row, dict) else "") or "")
    if op=="==":  return val == rhs
    if op=="!=":  return val != rhs
    if op=="~=":  return re.search(rhs, val) is not None
    if op=="!~=": return re.search(rhs, val) is None
    return True