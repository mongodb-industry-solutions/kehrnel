import re


_FIELD_PATTERN = re.compile(r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ.]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$')
_XPATH_COMPARISON_PATTERN = re.compile(r'^\s*xpath\(\s*([\'"])(.*?)\1\s*\)\s*(==|!=|~=|!~=)?\s*"?([^"]*)"?\s*$')


def _compare(lhs: str, op: str, rhs: str) -> bool:
    if op == "==":
        return lhs == rhs
    if op == "!=":
        return lhs != rhs
    if op == "~=":
        try:
            return re.search(rhs, lhs) is not None
        except re.error:
            return False
    if op == "!~=":
        try:
            return re.search(rhs, lhs) is None
        except re.error:
            return False
    return False


def evaluate(expr: str, *, row=None, vars=None) -> bool:
    row = row or {}
    vars = vars or {}

    m = _FIELD_PATTERN.match(expr or "")
    if m:
        key, op, rhs = m.groups()
        val = str((row.get(key) if isinstance(row, dict) else "") or "")
        return _compare(val, op, rhs)

    # Allow xpath('<expr>') and xpath('<expr>') <op> <rhs>
    if "xpath" in vars:
        x = _XPATH_COMPARISON_PATTERN.match(expr or "")
        if x:
            _, xpath_expr, op, rhs = x.groups()
            try:
                val = vars["xpath"](xpath_expr)
            except Exception:
                return False
            lhs = "" if val is None else str(val)
            # xpath('<expr>') with no operator means truthy check.
            if not op:
                return bool(val)
            return _compare(lhs, op, rhs or "")

    # Fail closed on unsupported expressions.
    return False
