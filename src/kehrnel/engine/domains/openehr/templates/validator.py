# ──────────────────────────────────────────────────────────────────────────────
#  core/validator.py
#  OpenEHR composition validator with comprehensive validation rules
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import Counter
from typing import Dict, List, Tuple, Optional, Any, Set, Callable
import re
import datetime as _dt

from .models import ValidationIssue, Severity
from .parser import TemplateParser

XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"

__all__ = ["kehrnelValidator"]

_RM_ISA: dict[str, set[str]] = {
    # EVENT is abstract; JSON instances are concrete POINT_EVENT / INTERVAL_EVENT.
    "EVENT": {"EVENT", "POINT_EVENT", "INTERVAL_EVENT"},
    "DV_TEXT": {"DV_TEXT", "DV_CODED_TEXT"},
    "DV_URI": {"DV_URI", "DV_EHR_URI"},
    "DV_ENCAPSULATED": {"DV_ENCAPSULATED", "DV_PARSABLE", "DV_MULTIMEDIA"},
    "DV_QUANTIFIED": {
        "DV_AMOUNT", "DV_ABSOLUTE_QUANTITY",
        "DV_QUANTITY", "DV_COUNT", "DV_PROPORTION", "DV_DURATION",
        "DV_DATE", "DV_TIME", "DV_DATE_TIME",
    },
    "DV_AMOUNT": {"DV_QUANTITY", "DV_COUNT", "DV_PROPORTION", "DV_DURATION"},
    "DV_ABSOLUTE_QUANTITY": {"DV_DATE", "DV_TIME", "DV_DATE_TIME"},
    "DV_ORDERED": {  # everything that inherits Ordered
        "DV_ORDINAL", "DV_QUANTITY", "DV_COUNT", "DV_PROPORTION",
        "DV_DURATION", "DV_DATE", "DV_TIME", "DV_DATE_TIME",
    },
}

class kehrnelValidator:
    """
    Validate a composition JSON object against an openEHR OPT template.
    """

    def _strip_generic(self, rm_type: str) -> str:
        """
        Remove generic parameter from an RM type name.

        Examples
        --------
        >>> self._strip_generic("DV_INTERVAL<DV_DATE_TIME>")
        'DV_INTERVAL'
        >>> self._strip_generic("DV_TEXT")
        'DV_TEXT'
        """
        return rm_type.split("<", 1)[0] if rm_type else rm_type
    
    def _types_match(self, expected: str, actual: str, jpath: str) -> bool:
        """
        Return True iff runtime type *actual* is allowed where template
        expects *expected* (takes openEHR inheritance & local overrides
        into account).
        """
        expected = self._strip_generic(expected or "")
        actual   = self._strip_generic(actual or "")

        # 0) identical
        if expected == actual:
            return True

        # 1) generic DV_INTERVAL<?> accepts any DV_INTERVAL<X>
        if expected.startswith("DV_INTERVAL") and actual.startswith("DV_INTERVAL"):
            return True

        # 2) inheritance lookup
        if expected in _RM_ISA and actual in _RM_ISA[expected]:
            return True

        # 3) template-specific overrides from <value> attrs (your old rule c)
        no_idx = re.sub(r'\[\d+\]', '', jpath)
        if no_idx in self._value_type_rules:
            return actual in self._value_type_rules[no_idx]

        return False

    def _unwrap_scalar(self, data: Any) -> Any:
        """
        • If data is a DV_*  RM object → return its 'value'.
        • Otherwise return data unchanged.
        """
        if isinstance(data, dict) and 'value' in data:
            return data['value']
        return data
        
    def __init__(self, template: TemplateParser) -> None:
        self.tpl: TemplateParser = template
        self._root_tag: str = self.tpl.tree.getroot().tag
        
        # Build parent map
        self._parent: Dict[ET.Element, ET.Element] = {
            child: parent for parent in self.tpl.tree.iter() for child in parent
        }
        
        # Pre-compute constraints
        self._mandatory_paths = self._extract_mandatory_paths()
        self._card_rules = self._extract_cardinality_rules()
        self._value_type_rules = self._extract_value_type_rules()
        self._occurrence_rules = self._extract_occurrence_rules()
        self._datatype_rules = self._extract_datatype_rules()
        self._terminology_rules = self._extract_terminology_rules()
        self._archetype_rules = self._extract_archetype_rules()
        self._name_constraints = self._extract_name_constraints()

        self._primitive_handlers: dict[str, Callable[[Any, ET.Element, str], list[ValidationIssue]]] = {
            "C_BOOLEAN":  self._check_boolean,
            "C_STRING":   self._check_string,
            "C_INTEGER":  self._check_integer,
            "C_REAL":     self._check_real,
            "C_DATE":     self._check_date,
            "C_DATE_TIME": self._check_datetime,
        }


    def validate(self, composition: dict) -> List[ValidationIssue]:
        """Return a list of ValidationIssue objects; empty list → valid."""
        issues: List[ValidationIssue] = []
        
        # 0. Basic sanity checks
        if not isinstance(composition, dict):
            return [ValidationIssue("/", "Composition must be a JSON object", Severity.ERROR, "VAL_STRUCT")]
            
        if composition.get("_type") != "COMPOSITION":
            issues.append(
                ValidationIssue(
                    "/_type", 
                    f'Root element must be COMPOSITION, got "{composition.get("_type")}"',
                    Severity.ERROR,
                    "VAL_ROOT"
                )
            )
        
        # 1. Template ID validation
        tpl_id = self.tpl.template_id
        comp_tpl_id = (composition.get("archetype_details", {})
                      .get("template_id", {})
                      .get("value"))
        
        # Strip whitespace for comparison as templates sometimes have extra spaces
        if tpl_id and comp_tpl_id and tpl_id.strip() != comp_tpl_id.strip():
            issues.append(
                ValidationIssue(
                    "/archetype_details/template_id/value",
                    f'Template id must be "{tpl_id.strip()}"',
                    Severity.ERROR,
                    "VAL_TEMPLATE",
                    expected=tpl_id.strip(),
                    found=comp_tpl_id
                )
            )
        
        # 2. Validate mandatory category
        category_code = composition.get("category", {}).get("defining_code", {}).get("code_string")
        if category_code != "433":
            issues.append(
                ValidationIssue(
                    "/category/defining_code/code_string",
                    'Category must be "433" (event)',
                    Severity.ERROR,
                    "VAL_CATEGORY",
                    expected="433",
                    found=category_code
                )
            )
        
        # 3. Validate structure recursively
        root_def = self.tpl.tree.find(".//opt:definition", self.tpl.NS)
        if root_def is not None:
            issues.extend(self._validate_node(composition, root_def, ""))
        else:
            issues.append(
                ValidationIssue(
                    "/",
                    "Template definition not found",
                    Severity.ERROR,
                    "VAL_TEMPLATE_DEF"
                )
            )
        
        return issues

    def _validate_node(self, data: Any, constraint: ET.Element, path: str) -> List[ValidationIssue]:
        """Recursively validate a data node against its constraint."""
        issues = []
        
        if constraint is None:
            return issues
        
        # Handle missing data for mandatory elements
        if data is None:
            occurrences = constraint.find("opt:occurrences", self.tpl.NS)
            if occurrences is not None:
                lower = int(occurrences.findtext("opt:lower", "0", self.tpl.NS))
                if lower >= 1:
                    node_id = constraint.findtext("opt:node_id", "", self.tpl.NS)
                    element_name = self._get_element_name(constraint)
                    issues.append(
                        ValidationIssue(
                            path,
                            f'Mandatory element "{element_name}" is missing',
                            Severity.ERROR,
                            "VAL_OCC",
                            expected=f"min {lower} occurrence(s)",
                            found="0"
                        )
                    )
            return issues
            
        # Get the RM type from constraint
        rm_type = constraint.findtext("opt:rm_type_name", "", self.tpl.NS)
        
        # Check data type matches
        if isinstance(data, dict):
            data_type = data.get("_type", "")
            if rm_type and data_type and not self._types_match(rm_type, data_type, path):
                issues.append(
                    ValidationIssue(
                        f"{path}/_type",
                        f'Expected type "{rm_type}"',
                        Severity.ERROR,
                        "VAL_TYPE",
                        expected=rm_type,
                        found=data_type
                    )
                )
        
        # Check occurrences for this node
        node_id = constraint.findtext("opt:node_id", "", self.tpl.NS)
        occurrences = constraint.find("opt:occurrences", self.tpl.NS)
        if occurrences is not None:
            lower = int(occurrences.findtext("opt:lower", "0", self.tpl.NS))
            if lower >= 1 and not data:
                element_name = self._get_element_name(constraint)
                issues.append(
                    ValidationIssue(
                        path,
                        f'Mandatory element "{element_name}" is missing',
                        Severity.ERROR,
                        "VAL_OCC",
                        expected=f"min {lower} occurrence(s)",
                        found="0"
                    )
                )
        
        # Validate attributes
        for attr in constraint.findall("opt:attributes", self.tpl.NS):
            attr_name = attr.findtext("opt:rm_attribute_name", "", self.tpl.NS)
            if not attr_name:
                continue
                
            attr_path = f"{path}/{attr_name}" if path else f"/{attr_name}"
            attr_data = data.get(attr_name) if isinstance(data, dict) else None
            
            # Check existence constraints
            existence = attr.find("opt:existence", self.tpl.NS)
            if existence is not None:
                lower = int(existence.findtext("opt:lower", "0", self.tpl.NS))
                if lower >= 1 and (attr_data is None or attr_data == ""):
                    issues.append(
                        ValidationIssue(
                            attr_path,
                            f'Mandatory attribute "{attr_name}" is missing',
                            Severity.ERROR,
                            "VAL_REQ"
                        )
                    )
            
            # Skip validation if attribute is missing and not mandatory
            if attr_data is None:
                continue
            
            # Handle multiple attributes (lists)
            if attr.get(XSI_TYPE) == "C_MULTIPLE_ATTRIBUTE":
                if not isinstance(attr_data, list):
                    issues.append(
                        ValidationIssue(
                            attr_path,
                            f'Expected list for "{attr_name}"',
                            Severity.ERROR,
                            "VAL_TYPE",
                            expected="list",
                            found=type(attr_data).__name__
                        )
                    )
                else:
                    all_children = [
                        child for child in attr
                        if child.tag.endswith("children") or "children" in child.tag
                    ]
                    card = attr.find("opt:cardinality", self.tpl.NS)
                    if card is not None and not all_children:          
                        lower = int(card.findtext("opt:interval/opt:lower", "0", self.tpl.NS))
                        upper_txt = card.findtext("opt:interval/opt:upper", "", self.tpl.NS)
                        upper = None if not upper_txt else int(upper_txt)

                        if len(attr_data) < lower:
                            issues.append(
                                ValidationIssue(
                                    attr_path,
                                    f"Too few items in {attr_name}",
                                    Severity.ERROR,
                                    "VAL_CARD_LOW",
                                    expected=f"min {lower}",
                                    found=str(len(attr_data)),
                                )
                            )
                        if upper is not None and len(attr_data) > upper:
                            issues.append(
                                ValidationIssue(
                                    attr_path,
                                    f"Too many items in {attr_name}",
                                    Severity.ERROR,
                                    "VAL_CARD_HIGH",
                                    expected=f"max {upper}",
                                    found=str(len(attr_data)),
                                )
                            )
                    
                    if all_children:
                        issues.extend(self._validate_multiple_attribute_items(
                            attr_data, attr, attr_path
                        ))
                    else:
                        # No constraints defined, just validate each item generically
                        for i, item in enumerate(attr_data):
                            item_path = f"{attr_path}[{i}]"
                            # Basic type validation only
                            if not isinstance(item, dict):
                                issues.append(
                                    ValidationIssue(
                                        item_path,
                                        "Item must be an object",
                                        Severity.ERROR,
                                        "VAL_TYPE"
                                    )
                                )
            
            # Handle single attributes
            elif attr.get(XSI_TYPE) == "C_SINGLE_ATTRIBUTE":
                for child_constraint in attr.findall("opt:children", self.tpl.NS):
                    # Special handling for primitive constraints
                    if child_constraint.get(XSI_TYPE) == "C_PRIMITIVE_OBJECT":
                        issues.extend(self._validate_primitive(attr_data, child_constraint, attr_path))
                    else:
                        issues.extend(self._validate_node(attr_data, child_constraint, attr_path))
        
        # Check archetype node ID if present
        if isinstance(data, dict) and "archetype_node_id" in data:
            expected_node_id = constraint.findtext("opt:node_id", "", self.tpl.NS)
            actual_node_id = data["archetype_node_id"]
            
            # Also check archetype_id for archetype roots
            archetype_id = constraint.findtext("opt:archetype_id/opt:value", "", self.tpl.NS)
            
            if expected_node_id and actual_node_id != expected_node_id:
                # Special case: archetype roots can use either node_id or archetype_id
                if not (archetype_id and actual_node_id == archetype_id):
                    issues.append(
                        ValidationIssue(
                            f"{path}/archetype_node_id",
                            f'Invalid archetype node ID',
                            Severity.ERROR,
                            "VAL_NODE_ID",
                            expected=expected_node_id,
                            found=actual_node_id
                        )
                    )
        
        # Validate name constraints
        if isinstance(data, dict) and "name" in data and constraint in self._name_constraints:
            expected_name = self._name_constraints[constraint]
            actual_name = data["name"].get("value") if isinstance(data["name"], dict) else None
            if actual_name != expected_name:
                issues.append(
                    ValidationIssue(
                        f"{path}/name/value",
                        f'Invalid name for element',
                        Severity.WARNING,
                        "VAL_NAME",
                        expected=expected_name,
                        found=actual_name
                    )
                )
        
        return issues

    def _extract_value_type_rules(self) -> Dict[str, set[str]]:
        """
        Map  json_path/to/value  →  { 'DV_TEXT', 'DV_CODED_TEXT', … }.
        """
        rules: Dict[str, set[str]] = {}
        # Grab *all* value-attributes and inspect their ancestors manually.
        for value_attr in self.tpl.tree.findall(
            ".//opt:attributes[@rm_attribute_name='value']", self.tpl.NS
        ):
            # Walk up until we either see an ELEMENT wrapper or hit the root.
            p = self._parent.get(value_attr)
            found_element_attr = False
            while p is not None and not found_element_attr:
                if (
                    p.tag.endswith("attributes")
                    and p.attrib.get("rm_attribute_name") == "items"
                ):
                    # The parent of this "items" attribute should be a
                    # C_COMPLEX_OBJECT with ELEMENT rm_type_name.
                    element_candidate = self._parent.get(p)
                    if (
                        element_candidate is not None
                        and element_candidate.findtext(
                            "opt:rm_type_name", "", self.tpl.NS
                        )
                        == "ELEMENT"
                    ):
                        found_element_attr = True
                        break
                p = self._parent.get(p)

            if not found_element_attr:
                continue
            path = self._build_json_path_for_attribute(value_attr)
            if not path:
                continue
            types = {
                child.findtext("opt:rm_type_name", "", self.tpl.NS)
                for child in value_attr.findall("opt:children", self.tpl.NS)
            }
            no_index_path = re.sub(r'\[\d+\]', '', path)
            rules[no_index_path] = {t for t in types if t} or {"DV_TEXT"}
        return rules
 
    def _validate_multiple_attribute_items(
        self,
        items: List[Any],
        attr: ET.Element,
        path: str
    ) -> List[ValidationIssue]:
        """
        Validate the list under a C_MULTIPLE_ATTRIBUTE.

        *   Tries to match every data item against one of the <children> constraints.
        *   Checks occurrences **per individual constraint** (upper & lower),
            not just the total list length.
        """
        issues: List[ValidationIssue] = []

        # collect the constraint elements (skip cardinality / existence nodes)
        child_constraints: List[ET.Element] = [
            c for c in attr
            if not any(tag in c.tag for tag in ("cardinality", "existence", "match_negated"))
        ]

        # Keep track of which constraint matched which item
        matched_constraints: List[Optional[ET.Element]] = []

        for i, item in enumerate(items):
            item_path = f"{path}[{i}]"
            matched = False

            for constraint in child_constraints:
                if self._could_match_constraint(item, constraint):
                    # recurse into the item with its matching constraint
                    issues.extend(self._validate_node(item, constraint, item_path))
                    matched_constraints.append(constraint)
                    matched = True
                    break

            if not matched and child_constraints:
                # Build a helpful error message
                item_type = item.get("_type", type(item).__name__) if isinstance(item, dict) else type(item).__name__
                item_node_id = item.get("archetype_node_id", "—") if isinstance(item, dict) else "—"
                options = [
                    f"{c.findtext('opt:rm_type_name', '', self.tpl.NS)}"
                    f"({c.findtext('opt:node_id', '', self.tpl.NS) or c.findtext('opt:archetype_id/opt:value', '', self.tpl.NS)})"
                    for c in child_constraints
                ]
                issues.append(
                    ValidationIssue(
                        item_path,
                        f"No matching constraint for {item_type} (node_id {item_node_id}). "
                        f"Available constraints: {', '.join(options)}",
                        Severity.ERROR,
                        "VAL_CONSTRAINT"
                    )
                )
                matched_constraints.append(None)


        hits = Counter(m for m in matched_constraints if m is not None)

        for constraint, seen in hits.items():
            occ = constraint.find("opt:occurrences", self.tpl.NS)
            if occ is None:
                continue

            lower = int(occ.findtext("opt:lower", "0", self.tpl.NS))
            upper_txt = occ.findtext("opt:upper", "", self.tpl.NS)
            upper = None if not upper_txt else int(upper_txt)

            node_id = constraint.findtext("opt:node_id", "unknown", self.tpl.NS)
            if seen < lower:
                issues.append(
                    ValidationIssue(
                        path,
                        f"Too few occurrences of node {node_id}",
                        Severity.ERROR,
                        "VAL_OCC_LOW",
                        expected=f"min {lower}",
                        found=str(seen),
                    )
                )
            if upper is not None and seen > upper:
                issues.append(
                    ValidationIssue(
                        path,
                        f"Too many occurrences of node {node_id}",
                        Severity.ERROR,
                        "VAL_OCC_HIGH",
                        expected=f"max {upper}",
                        found=str(seen),
                    )
                )

        return issues

    def _could_match_constraint(self, item: Any, constraint: ET.Element) -> bool:
        """Check if an item could potentially match a constraint."""
        if not isinstance(item, dict):
            return False
        
        # Skip meta-elements that aren't actual constraints
        if constraint.tag.endswith("rm_attribute_name"):
            return False
        
        # Get constraint details
        node_id = constraint.findtext("opt:node_id", "", self.tpl.NS)
        rm_type = constraint.findtext("opt:rm_type_name", "", self.tpl.NS)
        archetype_id = constraint.findtext("opt:archetype_id/opt:value", "", self.tpl.NS)
        
        # Get item details
        item_node_id = item.get("archetype_node_id")
        item_type = item.get("_type")
        item_archetype_id = item.get("archetype_details", {}).get("archetype_id", {}).get("value", "")
        
        # Priority 1: If both have node_id and they match exactly
        if node_id and item_node_id and node_id == item_node_id:
            return True
        
        # Priority 2: Check archetype_id matches
        if archetype_id:
            # Case 2a: Item has archetype_details.archetype_id.value
            if item_archetype_id == archetype_id:
                return True
            # Case 2b: Item uses archetype_node_id to store archetype_id (common pattern)
            if item_node_id == archetype_id:
                return True
        
        # Priority 3: For items with archetype_node_id that looks like an archetype ID
        # (e.g., "openEHR-EHR-SECTION.adhoc.v1"), try to match with archetype constraints
        if item_node_id and "openEHR-EHR-" in item_node_id and archetype_id == item_node_id:
            return True
        
        # Priority 4: Type matching when neither has specific identifiers
        if rm_type and item_type == rm_type:
            # Only match by type if there's no conflicting node_id or archetype_id
            if not (node_id and item_node_id and node_id != item_node_id):
                if not (archetype_id and item_archetype_id and archetype_id != item_archetype_id):
                    if not (archetype_id and item_node_id and archetype_id != item_node_id):
                        return True
        
        return False

    def _validate_primitive(self, data, constraint, path):
        item = constraint.find("opt:item", self.tpl.NS)
        if item is None:
            return []

        handler = self._primitive_handlers.get(item.get(XSI_TYPE))
        if handler is None:
            return []                           # unsupported subtype for now

        scalar = self._unwrap_scalar(data)
        return handler(scalar, item, path)

    def _check_string(self, value, item, path):
        issues = []
        if not isinstance(value, str):
            issues.append(ValidationIssue(path,
                "Expected string", Severity.ERROR, "VAL_TYPE",
                expected="string", found=type(value).__name__))
            return issues

        pattern = item.findtext("opt:pattern", "", self.tpl.NS)
        if pattern and not re.fullmatch(pattern, value):
            issues.append(ValidationIssue(
                path, "String does not match pattern",
                Severity.ERROR, "VAL_STR_PATTERN", expected=f"/{pattern}/", found=value))

        allowed = [n.text for n in item.findall("opt:list", self.tpl.NS) if n.text]
        if allowed and value not in allowed:
            issues.append(ValidationIssue(
                path, "String not in allowed value set",
                Severity.ERROR, "VAL_STR_ENUM",
                expected=", ".join(allowed), found=value))

        return issues

    @staticmethod
    def _extract_interval(item: ET.Element, ns: dict) -> tuple[Optional[float], Optional[float]]:
        """Return (lower, upper) limits as floats (None if unbounded)."""
        lo_txt = item.findtext("opt:lower", "", ns)
        hi_txt = item.findtext("opt:upper", "", ns)
        lo = None if lo_txt == "" else float(lo_txt)
        hi = None if hi_txt == "" else float(hi_txt)
        return lo, hi

    def _check_integer(self, value, item, path):
        issues = []
        if not isinstance(value, int):
            issues.append(ValidationIssue(path, "Expected integer",
                             Severity.ERROR, "VAL_TYPE",
                             expected="integer", found=type(value).__name__))
            return issues

        lo, hi = self._extract_interval(item, self.tpl.NS)
        if lo is not None and value < lo:
            issues.append(ValidationIssue(path, "Value below minimum",
                             Severity.ERROR, "VAL_MIN",
                             expected=str(lo), found=str(value)))
        if hi is not None and value > hi:
            issues.append(ValidationIssue(path, "Value above maximum",
                             Severity.ERROR, "VAL_MAX",
                             expected=str(hi), found=str(value)))
        return issues

    def _check_boolean(self, value, item, path):
        issues = []
        if not isinstance(value, bool):
            issues.append(ValidationIssue(path,
                "Expected boolean", Severity.ERROR, "VAL_TYPE",
                expected="boolean", found=type(value).__name__))
            return issues

        true_ok  = item.findtext("opt:true_valid",  "true", self.tpl.NS).lower() == "true"
        false_ok = item.findtext("opt:false_valid", "true", self.tpl.NS).lower() == "true"

        if  value and not true_ok:
            issues.append(ValidationIssue(
                path, "Value 'true' is not allowed",
                Severity.ERROR, "VAL_BOOL", expected="false", found="true"))
        if not value and not false_ok:
            issues.append(ValidationIssue(
                path, "Value 'false' is not allowed",
                Severity.ERROR, "VAL_BOOL", expected="true", found="false"))
        return issues

    def _check_real(self, value, item, path):
        """Validate C_REAL – a floating-point number with optional bounds."""
        issues = []
        # ---- type -----------------------------------------------------
        if not isinstance(value, (int, float)):
            issues.append(
                ValidationIssue(
                    path, "Expected real/float",
                    Severity.ERROR, "VAL_TYPE",
                    expected="real", found=type(value).__name__)
            )
            return issues

        # ---- range ----------------------------------------------------
        lo, hi = self._extract_interval(item, self.tpl.NS)
        val_f = float(value)
        if lo is not None and val_f < lo:
            issues.append(
                ValidationIssue(
                    path, "Value below minimum",
                    Severity.ERROR, "VAL_MIN",
                    expected=str(lo), found=str(value))
            )
        if hi is not None and val_f > hi:
            issues.append(
                ValidationIssue(
                    path, "Value above maximum",
                    Severity.ERROR, "VAL_MAX",
                    expected=str(hi), found=str(value))
            )
        return issues
        
    @staticmethod
    def _parse_iso_date(txt: str) -> Optional[_dt.date]:
        try:
            return _dt.date.fromisoformat(txt)
        except Exception:
            return None


    @staticmethod
    def _parse_iso_datetime(txt: str) -> Optional[_dt.datetime]:
        # allows both “YYYY-MM-DDThh:mm:ss” and the space variant
        txt = txt.replace(" ", "T", 1)
        try:
            return _dt.datetime.fromisoformat(txt)
        except Exception:
            return None


    def _compare_bounds(self, value_dt, lower_txt, upper_txt):
        """Return (is_too_low, is_too_high) for the parsed datetime/date."""
        too_low = too_high = False
        if lower_txt:
            lo = self._parse_iso_datetime(lower_txt) or self._parse_iso_date(lower_txt)
            if lo and value_dt < lo:
                too_low = True
        if upper_txt:
            hi = self._parse_iso_datetime(upper_txt) or self._parse_iso_date(upper_txt)
            if hi and value_dt > hi:
                too_high = True
        return too_low, too_high


    def _check_date(self, value, item, path):
        """Validate C_DATE – an ISO-8601 calendar date."""
        issues = []
        if not isinstance(value, str):
            issues.append(
                ValidationIssue(
                    path, "Expected ISO date string",
                    Severity.ERROR, "VAL_TYPE",
                    expected="YYYY-MM-DD", found=type(value).__name__)
            )
            return issues

        date_val = self._parse_iso_date(value)
        if date_val is None:
            issues.append(
                ValidationIssue(
                    path, "Invalid date format",
                    Severity.ERROR, "VAL_DATE_FMT", found=value)
            )
            return issues

        lower_txt = item.findtext("opt:lower", "", self.tpl.NS)
        upper_txt = item.findtext("opt:upper", "", self.tpl.NS)
        low, high = _compare_bounds(self, date_val, lower_txt, upper_txt)
        if low:
            issues.append(
                ValidationIssue(
                    path, "Date before minimum",
                    Severity.ERROR, "VAL_MIN", expected=lower_txt, found=value)
            )
        if high:
            issues.append(
                ValidationIssue(
                    path, "Date after maximum",
                    Severity.ERROR, "VAL_MAX", expected=upper_txt, found=value)
            )
        return issues


    def _check_datetime(self, value, item, path):
        """Validate C_DATE_TIME – an ISO-8601 date-time."""
        issues = []
        if not isinstance(value, str):
            issues.append(
                ValidationIssue(
                    path, "Expected ISO date-time string",
                    Severity.ERROR, "VAL_TYPE",
                    expected="YYYY-MM-DDThh:mm:ss", found=type(value).__name__)
            )
            return issues

        dt_val = self._parse_iso_datetime(value)
        if dt_val is None:
            issues.append(
                ValidationIssue(
                    path, "Invalid date-time format",
                    Severity.ERROR, "VAL_DT_FMT", found=value)
            )
            return issues

        lower_txt = item.findtext("opt:lower", "", self.tpl.NS)
        upper_txt = item.findtext("opt:upper", "", self.tpl.NS)
        low, high = _compare_bounds(self, dt_val, lower_txt, upper_txt)
        if low:
            issues.append(
                ValidationIssue(
                    path, "Date-time before minimum",
                    Severity.ERROR, "VAL_MIN", expected=lower_txt, found=value)
            )
        if high:
            issues.append(
                ValidationIssue(
                    path, "Date-time after maximum",
                    Severity.ERROR, "VAL_MAX", expected=upper_txt, found=value)
            )
        return issues
        
    def _validate_slot(self, data: Any, slot: ET.Element, path: str) -> List[ValidationIssue]:
        """Validate archetype slot constraints."""
        issues = []
        
        if not isinstance(data, dict):
            return issues
        
        archetype_id = data.get("archetype_details", {}).get("archetype_id", {}).get("value", "")
        if not archetype_id:
            archetype_id = data.get("archetype_node_id", "")
        
        # Get includes pattern
        includes = slot.find("opt:includes/opt:string_expression", self.tpl.NS)
        if includes is not None and includes.text:
            # Extract pattern from the expression
            match = re.search(r'archetype_id/value matches \{/(.*)/\}', includes.text)
            if match:
                pattern = match.group(1)
                if not re.match(pattern, archetype_id):
                    issues.append(
                        ValidationIssue(
                            f"{path}/archetype_details/archetype_id/value",
                            f"Archetype ID does not match slot constraint",
                            Severity.ERROR,
                            "VAL_SLOT",
                            expected=f"matches /{pattern}/",
                            found=archetype_id
                        )
                    )
        
        return issues

    def _get_element_name(self, constraint: ET.Element) -> str:
        """Get human-readable name for an element from term definitions."""
        node_id = constraint.findtext("opt:node_id", "", self.tpl.NS)
        if node_id and node_id in self.tpl.term_definitions:
            return self.tpl.term_definitions[node_id]
        
        # Fallback to RM type name
        rm_type = constraint.findtext("opt:rm_type_name", "", self.tpl.NS)
        return rm_type or "element"

    def _extract_name_constraints(self) -> Dict[ET.Element, str]:
        """Extract expected names for elements with specific node IDs."""
        constraints = {}
        
        # Find all elements that might have name constraints
        # We need to iterate through all elements and check their attributes manually
        for elem in self.tpl.tree.iter():
            node_id = elem.findtext("opt:node_id", "", self.tpl.NS)
            if not node_id:
                continue
            
            # Only capture the *first* name/value constraint inside this element
            # and tie it to the *full* XML-path so it can't bleed onto siblings
            for attr in elem.findall("opt:attributes", self.tpl.NS):
                if attr.findtext("opt:rm_attribute_name", "", self.tpl.NS) == "name":
                    # Found a name attribute, look for constraints
                    for child in attr.iter():
                        if (child.tag.endswith("item") and 
                            child.get(XSI_TYPE) == "C_STRING"):
                            # Found C_STRING constraint
                            list_elem = child.find("opt:list", self.tpl.NS)
                            if list_elem is not None and list_elem.text:
                                constraints[elem] = list_elem.text
                                break
        
        return constraints

    def _extract_mandatory_paths(self) -> Set[str]:
        """Extract all mandatory paths from the template."""
        mandatory = set()
        
        # Find all elements with min occurrence >= 1
        for elem in self.tpl.tree.findall(".//*[opt:occurrences]", self.tpl.NS):
            occurrences = elem.find("opt:occurrences", self.tpl.NS)
            if occurrences is not None:
                lower = int(occurrences.findtext("opt:lower", "0", self.tpl.NS))
                if lower >= 1:
                    path = self._build_path_to_element(elem)
                    if path:
                        mandatory.add(path)
        
        # Find all attributes with existence min >= 1
        for attr in self.tpl.tree.findall(".//opt:attributes[opt:existence]", self.tpl.NS):
            existence = attr.find("opt:existence", self.tpl.NS)
            if existence is not None:
                lower = int(existence.findtext("opt:lower", "0", self.tpl.NS))
                if lower >= 1:
                    path = self._build_json_path_for_attribute(attr)
                    if path:
                        mandatory.add(path)
        
        return mandatory

    def _extract_occurrence_rules(self) -> Dict[str, Tuple[int, Optional[int]]]:
        """Extract occurrence constraints by node_id."""
        rules = {}
        
        for elem in self.tpl.tree.findall(".//*[opt:node_id]", self.tpl.NS):
            node_id = elem.findtext("opt:node_id", "", self.tpl.NS)
            if not node_id:
                continue
                
            occurrences = elem.find("opt:occurrences", self.tpl.NS)
            if occurrences is not None:
                lower = int(occurrences.findtext("opt:lower", "0", self.tpl.NS))
                upper_txt = occurrences.findtext("opt:upper", "", self.tpl.NS)
                upper = None if not upper_txt else int(upper_txt)
                rules[node_id] = (lower, upper)
        
        return rules

    def _extract_datatype_rules(self) -> Dict[str, str]:
        """Extract expected data types for paths."""
        rules = {}
        
        for elem in self.tpl.tree.findall(".//*[@xsi:type='C_COMPLEX_OBJECT']", self.tpl.NS):
            rm_type = elem.findtext("opt:rm_type_name", "", self.tpl.NS)
            if rm_type:
                path = self._build_path_to_element(elem)
                if path:
                    rules[path] = rm_type
        
        return rules

    def _extract_terminology_rules(self) -> Dict[str, Set[str]]:
        """Extract terminology constraints (allowed code values)."""
        rules = {}
        
        for code_phrase in self.tpl.tree.findall(".//opt:children[@xsi:type='C_CODE_PHRASE']", self.tpl.NS):
            # Get allowed codes
            codes = set()
            for code_elem in code_phrase.findall("opt:code_list", self.tpl.NS):
                if code_elem.text:
                    codes.add(code_elem.text)
            
            if codes:
                # Find the attribute this constraint belongs to
                attr = self._find_parent_attribute(code_phrase)
                if attr:
                    path = self._build_json_path_for_attribute(attr)
                    if path:
                        rules[path] = codes
        
        return rules

    def _extract_archetype_rules(self) -> Dict[str, str]:
        """Extract archetype ID constraints for slots."""
        rules = {}
        
        for slot in self.tpl.tree.findall(".//ARCHETYPE_SLOT", self.tpl.NS):
            node_id = slot.findtext("opt:node_id", "", self.tpl.NS)
            if node_id:
                # Extract allowed archetype patterns from includes
                includes = slot.find("opt:includes/opt:string_expression", self.tpl.NS)
                if includes is not None and includes.text:
                    # Parse the regex pattern
                    match = re.search(r'archetype_id/value matches \{/(.*)/\}', includes.text)
                    if match:
                        rules[node_id] = match.group(1)
        
        return rules

    def _extract_cardinality_rules(self) -> Dict[str, Tuple[int, Optional[int]]]:
        """Extract cardinality constraints for lists."""
        rules = {}
        
        for attr in self.tpl.tree.findall(".//opt:attributes[@xsi:type='C_MULTIPLE_ATTRIBUTE']", self.tpl.NS):
            card = attr.find("opt:cardinality", self.tpl.NS)
            if card is not None:
                lower = int(card.findtext("opt:interval/opt:lower", "0", self.tpl.NS))
                upper_txt = card.findtext("opt:interval/opt:upper", "", self.tpl.NS)
                upper = None if not upper_txt else int(upper_txt)
                
                path = self._build_json_path_for_attribute(attr)
                if path:
                    rules[path] = (lower, upper)
        
        return rules

    def _find_parent_attribute(self, elem: ET.Element) -> Optional[ET.Element]:
        """Find the parent attribute element."""
        current = self._parent.get(elem)
        while current is not None:
            if current.tag.endswith("attributes"):
                return current
            current = self._parent.get(current)
        return None

    def _build_json_path_for_attribute(self, attr_el: ET.Element) -> Optional[str]:
        """Build JSON path for an attribute element."""
        parts = []
        current = attr_el
        
        while current is not None and current.tag != self._root_tag:
            if current.tag.endswith("attributes"):
                name = current.findtext("opt:rm_attribute_name", "", self.tpl.NS)
                parts.append(name)
                if current.attrib.get(XSI_TYPE, "").startswith("C_MULTIPLE_ATTRIBUTE"):
                    parts.append("0")    

            current = self._parent.get(current)
        
        if not parts:
            return None
            
        parts.reverse()
        return "/" + "/".join(parts)

    def _build_path_to_element(self, elem: ET.Element) -> Optional[str]:
        """Build path to any element in the template."""
        parts = []
        current = elem
        
        while current is not None and current.tag != self._root_tag:
            # Add attribute names to path
            parent_attr = self._find_parent_attribute(current)
            if parent_attr:
                attr_name = parent_attr.findtext("opt:rm_attribute_name", "", self.tpl.NS)
                if attr_name and attr_name not in parts:
                    parts.append(attr_name)
            current = self._parent.get(current)
        
        if not parts:
            return None
            
        parts.reverse()
        return "/" + "/".join(parts)
