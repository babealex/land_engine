from typing import Dict, Any, List

def check_condition(parcel: Dict[str, Any], field: str, operator: str, value: str) -> bool:
    """
    Evaluate a single eligibility condition on a parcel dict.

    Supported operators (string, case-insensitive):
      - 'in'           : comma-separated allowed values (string compare)
      - '==' / '!='    : equality / inequality (string or numeric)
      - '>' '<' '>=' '<=' : numeric comparisons when possible
      - 'between'      : numeric range, value formatted as 'min,max'
    """
    v = parcel.get(field, None)

    # Handle 'in' as a pure string membership check
    op = operator.strip().lower()
    if op == "in":
        allowed = [x.strip() for x in str(value).split(",")]
        return str(v) in allowed

    # Direct string-based equality / inequality
    if op == "==":
        return str(v) == str(value)
    if op == "!=":
        return str(v) != str(value)

    # Try numeric comparison for the remaining operators
    try:
        v_num = float(v)  # may raise TypeError/ValueError
    except (TypeError, ValueError):
        # If we cannot interpret the parcel value as a number, the condition fails
        return False

    # 'between' expects 'min,max'
    if op == "between":
        try:
            parts = [p.strip() for p in str(value).split(",")]
            if len(parts) != 2:
                return False
            lo = float(parts[0])
            hi = float(parts[1])
        except (TypeError, ValueError):
            return False
        return lo <= v_num <= hi

    # All other numeric comparators expect a single numeric 'value'
    try:
        val_num = float(value)
    except (TypeError, ValueError):
        return False

    if op == ">":
        return v_num > val_num
    if op == "<":
        return v_num < val_num
    if op == ">=":
        return v_num >= val_num
    if op == "<=":
        return v_num <= val_num

    # Unknown operator -> fail safe
    return False


def build_crp_rule_map(df) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert a CRP eligibility rules DataFrame into a rule_map:

        {
          'CP01': [
              {'field': 'land_cover', 'operator': 'in', 'value': 'cropland,pasture'},
              {'field': 'slope_percent', 'operator': '<=', 'value': '8'},
          ],
          'CP23': [ ... ],
          ...
        }

    Expected DataFrame columns (lowercase):
      - 'crp_practice_code'
      - 'field_name'
      - 'operator'
      - 'value'

    If these columns are missing, an empty map is returned.
    """
    if df is None:
        return {}

    # Normalize column names to lowercase, strip spaces
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    required = {"crp_practice_code", "field_name", "operator", "value"}
    if not required.issubset(df.columns):
        # Sheet not in the expected format; don't crash, just disable rules.
        return {}

    rule_map: Dict[str, List[Dict[str, Any]]] = {}
    for _, row in df.iterrows():
        code = str(row.get("crp_practice_code", "")).strip()
        field = str(row.get("field_name", "")).strip()
        operator = str(row.get("operator", "")).strip()
        value = str(row.get("value", "")).strip()

        if not code or not field or not operator:
            # skip incomplete rows
            continue

        cond = {
            "field": field,
            "operator": operator,
            "value": value,
        }
        rule_map.setdefault(code, []).append(cond)

    return rule_map


def eligible_crp_practices(parcel: Dict[str, Any], rule_map: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    """
    Given a parcel dict and a rule_map (as produced by build_crp_rule_map),
    return the list of CRP practice codes for which *all* conditions pass.

    If rule_map is empty, this returns an empty list; the caller may treat
    that as 'no restriction' and simply not apply any rule-based filter.
    """
    if not rule_map:
        return []

    eligible: List[str] = []
    for code, conditions in rule_map.items():
        if all(check_condition(parcel, c["field"], c["operator"], c["value"]) for c in conditions):
            eligible.append(code)
    return eligible
