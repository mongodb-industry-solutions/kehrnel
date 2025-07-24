# generator.py
#!/usr/bin/env python3

"""
kehrnelGenerator Core Module 
Handles OPT parsing, constraint validation, and composition generation for ANY OPT template
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
from .parser import TemplateParser
from xml.etree import ElementTree as ET
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import random
import datetime as dt
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from mapper.mapping_engine import SourceHandler, apply_mapping


# ════════════════════════════════════════════════════════════════════════════
# Data Models for Constraints
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Cardinality:
    min: int
    max: Optional[int]  # None means unbounded
    ordered: bool = False
    unique: bool = False

@dataclass
class Occurrence:
    min: int
    max: Optional[int]  # None means unbounded

@dataclass
class CodeConstraint:
    terminology: str
    codes: List[str]
    
@dataclass
class StringConstraint:
    pattern: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    max_length: Optional[int] = None

@dataclass
class NumericConstraint:
    min: Optional[float] = None
    max: Optional[float] = None

class ConstraintType(Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    PROHIBITED = "prohibited"

# ════════════════════════════════════════════════════════════════════════════
# Source Handler Interface
# ════════════════════════════════════════════════════════════════════════════

class SourceHandler(ABC):
    """Abstract base class for source format handlers"""
    
    @abstractmethod
    def can_handle(self, source_path: Path) -> bool:
        """Check if this handler can process the source file"""
        pass
    
    @abstractmethod
    def extract_value(self, source_data: Any, extraction_rule: Any) -> Any:
        """Extract a value from the source using the extraction rule"""
        pass
    
    @abstractmethod
    def load_source(self, source_path: Path) -> Any:
        """Load the source file"""
        pass
    
    @abstractmethod
    def count_elements(self, source_data: Any, xpath_or_path: str) -> int:
        """Count elements matching the path"""
        pass

# ════════════════════════════════════════════════════════════════════════════
# Main Generator Class
# ════════════════════════════════════════════════════════════════════════════

class kehrnelGenerator:
    """Main generator class that coordinates constraint-aware generation"""
    
    NS = {
        "opt": "http://schemas.openehr.org/v1",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }
    
    def __init__(self, template: TemplateParser):
        self.tpl             = template
        self.opt_path = template.opt_path
        self.tree            = template.tree
        self.handlers: List[SourceHandler] = []
        
        # Build term definition maps for each archetype
        self.term_definitions = self._build_term_map()
        self.archetype_terms = self._build_archetype_term_maps()
        
        # Extract template metadata
        self.template_id = self.tree.findtext(".//opt:template_id/opt:value", "", self.NS).strip() 

        # Current archetype context during processing
        self.current_archetype_id = None
        
    def _build_archetype_term_maps(self) -> Dict[str, Dict[str, str]]:
        """Build separate term maps for each archetype"""
        archetype_terms = {}
        
        # Find all archetype roots
        for archetype_root in self.tree.findall(".//opt:children[@xsi:type='C_ARCHETYPE_ROOT']", self.NS):
            archetype_id_elem = archetype_root.find("opt:archetype_id/opt:value", self.NS)
            if archetype_id_elem is not None:
                archetype_id = archetype_id_elem.text
                
                # Build term map for this archetype
                archetype_term_map = {}
                for term_def in archetype_root.findall(".//opt:term_definitions", self.NS):
                    code = term_def.get("code", "")
                    for item in term_def.findall("opt:items", self.NS):
                        if item.get("id") == "text" and item.text:
                            archetype_term_map[code] = item.text.strip()
                            break
                
                archetype_terms[archetype_id] = archetype_term_map
                print(f"Found {len(archetype_term_map)} terms for archetype {archetype_id}")
                
                # Debug: print the specific terms we care about
                if "openEHR-EHR-CLUSTER.igr_pmsi_stay_segment_cluster.v0" in archetype_id:
                    print("Cluster archetype terms:")
                    for code in ["at0001", "at0002", "at0003"]:
                        if code in archetype_term_map:
                            print(f"  {code}: {archetype_term_map[code]}")
                        else:
                            print(f"  {code}: NOT FOUND")
                
        return archetype_terms
        
    def _build_term_map(self) -> Dict[str, str]:
        """Build a map of term codes to their definitions from template level"""
        term_map = {}
        
        # Find template-level term definitions only
        template_def = self.tree.find(".//opt:definition", self.NS)
        if template_def is not None:
            for term_def in template_def.findall(".//opt:term_definitions", self.NS):
                code = term_def.get("code", "")
                for item in term_def.findall("opt:items", self.NS):
                    if item.get("id") == "text" and item.text:
                        term_map[code] = item.text.strip()
                        break
        
        return term_map
        
    def register_handler(self, handler: SourceHandler):
        """Register a source format handler"""
        self.handlers.append(handler)
        
    def generate_random(self) -> Dict:
        """Generate a random composition respecting all constraints"""
        # Parse the OPT template definition
        root_def = self.tree.find(".//opt:definition", self.NS)
        if root_def is None:
            raise ValueError("No definition found in OPT template")
        
        # Debug: Print some info about what we found
        print(f"Found {len(self.term_definitions)} term definitions in template")
        
        # Generate the composition structure
        composition = self._process_template_node(root_def, "/", 0)
        
        # Add template metadata to composition
        if composition and composition.get("_type") == "COMPOSITION":
            # Set template name
            if self.template_id and "name" not in composition:
                composition["name"] = {"_type": "DV_TEXT", "value": self.template_id}
            
            # Ensure archetype_details includes template_id
            if "archetype_details" in composition:
                if self.template_id and "template_id" not in composition["archetype_details"]:
                    composition["archetype_details"]["template_id"] = {"value": self.template_id}
            
        return composition
    
    def _get_term_for_node(self, node_id: str, rm_type: str) -> Optional[str]:
        """Get the appropriate term for a node based on current archetype context"""
        if not node_id or not self._rm_type_has_name(rm_type):
            return None
            
        # First check current archetype terms
        if self.current_archetype_id and self.current_archetype_id in self.archetype_terms:
            if node_id in self.archetype_terms[self.current_archetype_id]:
                term = self.archetype_terms[self.current_archetype_id][node_id]
                print(f"    Using archetype term: {node_id} -> {term}")
                return term
        
        # Fall back to template terms
        if node_id in self.term_definitions:
            term = self.term_definitions[node_id]
            print(f"    Using template term: {node_id} -> {term}")
            return term
        
        return None
    
    def _process_template_node(self, node: ET.Element, path: str, depth: int = 0) -> Optional[Dict]:
        """Process a template node and generate its structure"""
        # ---- skip archetype-slots -----------------------------------------
        xsi_type = node.get(f"{{{self.NS['xsi']}}}type")
        if xsi_type in ("ARCHETYPE_SLOT", "C_ARCHETYPE_SLOT"):
            # optional slot – leave it empty, a real archetype can be
            # inserted later via a mapping or explicit input
            return None
        # -------------------------------------------------------------------
        rm_type = node.findtext("opt:rm_type_name", "", self.NS)
        node_id = node.findtext("opt:node_id", "", self.NS).strip()
        
        if not rm_type:
            return None
        
        # Debug output for understanding structure
        indent = "  " * depth
        if depth < 4:
            print(f"{indent}Processing: {rm_type} (node_id: {node_id}) at {path}")
        
        # Check if this is an archetype root and update context
        archetype_id = node.findtext("opt:archetype_id/opt:value", "", self.NS)
        is_archetype_root = node.get(f"{{{self.NS['xsi']}}}type") == "C_ARCHETYPE_ROOT"
        
        old_archetype_id = self.current_archetype_id
        if archetype_id:
            self.current_archetype_id = archetype_id
            if depth < 4:
                print(f"{indent}  Entering archetype: {archetype_id}")
        
        # Create base structure with _type always first
        result = OrderedDict([("_type", rm_type)])
        term = self._get_term_for_node(node_id, rm_type)
        if term:
            result["name"] = {"_type": "DV_TEXT", "value": term}

        if archetype_id and rm_type != "COMPOSITION":
            result["archetype_details"] = {
                "archetype_id": {"value": archetype_id},
                "rm_version": "1.0.4"
            }

        # -------------------------------------------------------------
        #  ▶  Special-case fixes that depend on the name
        # -------------------------------------------------------------
        if rm_type == "ITEM_TREE" and result.get("name", {}).get("value") == "Tree" and "data" in path:
            result["name"]["value"] = "Item tree"

        # -------------------------------------------------------------
        #  ▶  COMPOSITION root: archetype_details, language, …
        # -------------------------------------------------------------
        if rm_type == "COMPOSITION":
            # archetype_details first
            result["archetype_details"] = {
                "archetype_id": {"value": archetype_id},
                "rm_version": "1.0.4"
            }
            if self.template_id:
                result["archetype_details"]["template_id"] = {"value": self.template_id}

            # mandatory RM fields
            result["language"] = {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
                "code_string": "en"
            }
            result["territory"] = {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_3166-1"},
                "code_string": "DE"
            }
            result["category"] = {
                "_type": "DV_CODED_TEXT",
                "value": "event",
                "defining_code": {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                    "code_string": "433"
                }
            }
            result["composer"] = {"_type": "PARTY_IDENTIFIED", "name": "Max Mustermann"}

        # -------------------------------------------------------------
        #  ▶  Non-COMPOSITION root: mandatory RM fields
        #      (inserted BEFORE template attributes 
        # -------------------------------------------------------------
        if rm_type != "COMPOSITION":
            self._add_required_rm_fields(result, rm_type)

        attributes_to_add = {}
        
        for attr in node.findall("opt:attributes", self.NS):
            attr_name = attr.findtext("opt:rm_attribute_name", "", self.NS)
            if not attr_name:
                continue
            
            if attr_name == "name" and "name" in result:
                continue
            
            # Special handling for category attribute in COMPOSITION
            if attr_name == "category" and rm_type == "COMPOSITION":
                value = self._process_single_attribute(attr, f"{path}/{attr_name}", depth, rm_type)
                if value and value.get("_type") == "DV_CODED_TEXT":
                    if "defining_code" in value and "value" not in value:
                        code_string = value["defining_code"].get("code_string", "433")
                        if code_string == "433":
                            value["value"] = "event"
                        else:
                            value["value"] = code_string
                if value is not None:
                    attributes_to_add[attr_name] = value
                continue
            
            # Check if it's a multiple attribute
            is_multiple = attr.get(f"{{{self.NS['xsi']}}}type") == "C_MULTIPLE_ATTRIBUTE"
            
            if is_multiple:
                items = self._process_multiple_attribute(attr, f"{path}/{attr_name}", depth)
                if items:
                    attributes_to_add[attr_name] = items
            else:
                # Single attribute
                value = self._process_single_attribute(attr, f"{path}/{attr_name}", depth, rm_type)
                if value is not None:
                    attributes_to_add[attr_name] = value
        
        # Add attributes in the correct order for COMPOSITION
        if rm_type == "COMPOSITION":
            # Category is already added above, so skip it here
            # Add context and content
            for key in ["context", "content"]:
                if key in attributes_to_add:
                    result[key] = attributes_to_add.pop(key)
            
            # Add any remaining attributes
            for key, value in attributes_to_add.items():
                if key != "category":  # Skip category since it's already added
                    result[key] = value
        else:
            # For non-COMPOSITION elements, add all attributes normally
            for key, value in attributes_to_add.items():
                result[key] = value
        
        # Add archetype information for non-COMPOSITION archetype roots
        if archetype_id and rm_type != "COMPOSITION":
            result["archetype_details"] = {
                "archetype_id": {"value": archetype_id},
                "rm_version": "1.0.4"
            }
        
        # Add archetype_node_id as the LAST element
        if archetype_id:
            result["archetype_node_id"] = archetype_id
        elif node_id:
            result["archetype_node_id"] = node_id
        
        # Restore archetype context
        self.current_archetype_id = old_archetype_id
        
        return result
    
    def _process_multiple_attribute(self, attr: ET.Element, path: str, depth: int) -> List[Dict]:
        """Process a multiple attribute (array)"""
        items = []
        
        # Get cardinality constraints
        card_elem = attr.find("opt:cardinality", self.NS)
        min_items = 1
        max_items = 20
        
        if card_elem is not None:
            interval = card_elem.find("opt:interval", self.NS)
            if interval is not None:
                min_items = int(interval.findtext("opt:lower", "1", self.NS))
                max_upper = interval.findtext("opt:upper", "")
                if max_upper and max_upper != "*":
                    max_items = int(max_upper)
                else:
                    max_items = 20
        
        # Process children
        children = attr.findall("opt:children", self.NS)
        if children:
            for child in children:
                child_occ = child.find("opt:occurrences", self.NS)
                child_min = 1
                if child_occ is not None:
                    child_min = max(1, int(child_occ.findtext("opt:lower", "1", self.NS)))
                
                for _ in range(child_min):
                    if len(items) >= max_items:
                        break
                    
                    child_result = self._process_template_node(child, path, depth + 1)
                    if child_result:
                        items.append(child_result)
                
                if len(items) >= max_items:
                    break
        
        return items
    
    def _process_single_attribute(self, attr: ET.Element, path: str, depth: int, parent_rm_type: str) -> Any:
        """Process a single attribute"""
        child = attr.find("opt:children", self.NS)
        if child is None:
            return None
        
        # Check if this is a complex object or primitive constraint
        child_rm_type = child.findtext("opt:rm_type_name", "", self.NS)
        
        if child_rm_type:
            # Complex object
            attr_name = attr.findtext("opt:rm_attribute_name", "", self.NS)
            
            # Special handling for different attribute types
            if attr_name == "value" and parent_rm_type == "ELEMENT":
                return self._process_data_value(child, path)
            elif child_rm_type in ["DV_CODED_TEXT", "DV_TEXT", "DV_BOOLEAN", "DV_COUNT", "DV_DATE_TIME", "DV_QUANTITY"]:
                return self._process_data_value(child, path)
            else:
                return self._process_template_node(child, path, depth + 1)
        else:
            # Primitive constraint
            child_type = child.get(f"{{{self.NS['xsi']}}}type", "")
            
            if child_type == "C_CODE_PHRASE":
                return self._process_code_phrase_constraint(child)
            elif child_type == "C_PRIMITIVE_OBJECT":
                for item in child.findall(".//opt:item", self.NS):
                    item_type = item.get(f"{{{self.NS['xsi']}}}type")
                    if item_type:
                        return self._process_primitive_constraint(item, item_type)
            elif child_type:
                return self._process_primitive_constraint(child, child_type)
        
        return None
    
    def _process_data_value(self, node: ET.Element, path: str) -> Optional[Dict]:
        """
        Build a DV_* object from the constraint subtree stored in *node*.

        • Looks at the constraint → picks an example value that satisfies it.
        • For DV_CODED_TEXT it resolves the human-readable text by
          – first checking the *current archetype* term map
          – then the *template-level* term map
          – finally falling back to the code string itself.
        """
        rm_type = node.findtext("opt:rm_type_name", "", self.NS)
        if not rm_type:
            return None

        result: Dict[str, Any] = {"_type": rm_type}

        # ──────────────────────────────────────────────────────────────
        # DV_CODED_TEXT
        # ──────────────────────────────────────────────────────────────
        if rm_type == "DV_CODED_TEXT":
            # 1) pick / build a CODE_PHRASE ----------------------------
            defining_code = None
            for attr in node.findall("opt:attributes", self.NS):
                if attr.findtext("opt:rm_attribute_name", "", self.NS) == "defining_code":
                    child = attr.find("opt:children", self.NS)
                    if child is not None:
                        defining_code = self._process_code_phrase_constraint(child)
                        break

            if defining_code is None:
                # no explicit constraint → fabricate a local dummy code
                defining_code = {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"},
                    "code_string": "at0000"
                }

            result["defining_code"] = defining_code

            # 2) human readable text -----------------------------------
            code_string = defining_code["code_string"]
            terminology = defining_code["terminology_id"]["value"]

            term = None
            if (self.current_archetype_id and
                    self.current_archetype_id in self.archetype_terms and
                    code_string in self.archetype_terms[self.current_archetype_id]):
                term = self.archetype_terms[self.current_archetype_id][code_string]
            elif code_string in self.term_definitions:
                term = self.term_definitions[code_string]

            # special case for COMPOSITION.category (spec says 'event')
            if terminology == "openehr" and code_string == "433":
                term = term or "event"

            result["value"] = term or code_string
            return result

        # ──────────────────────────────────────────────────────────────
        # DV_TEXT
        # ──────────────────────────────────────────────────────────────
        if rm_type == "DV_TEXT":
            # Is there a C_STRING constraint?
            for attr in node.findall("opt:attributes", self.NS):
                if attr.findtext("opt:rm_attribute_name", "", self.NS) == "value":
                    child = attr.find("opt:children", self.NS)
                    if child is not None and child.get(f"{{{self.NS['xsi']}}}type") == "C_STRING":
                        result["value"] = self._process_primitive_constraint(child, "C_STRING")
                        return result
            result["value"] = "Lorem ipsum"
            return result

        # ──────────────────────────────────────────────────────────────
        # DV_BOOLEAN
        # ──────────────────────────────────────────────────────────────
        if rm_type == "DV_BOOLEAN":
            result["value"] = True
            return result

        # ──────────────────────────────────────────────────────────────
        # DV_COUNT
        # ──────────────────────────────────────────────────────────────
        if rm_type == "DV_COUNT":
            for attr in node.findall("opt:attributes", self.NS):
                if attr.findtext("opt:rm_attribute_name", "", self.NS) == "magnitude":
                    child = attr.find("opt:children", self.NS)
                    if child is not None:
                        val = self._process_primitive_constraint(child, "C_INTEGER")
                        if val is not None:
                            result["magnitude"] = val
                            return result
            result["magnitude"] = 50
            return result

        # ──────────────────────────────────────────────────────────────
        # DV_QUANTITY
        # ──────────────────────────────────────────────────────────────
        if rm_type == "DV_QUANTITY":
            result["magnitude"] = 1.0
            result["units"] = "1"
            return result

        # ──────────────────────────────────────────────────────────────
        # DV_DATE_TIME
        # ──────────────────────────────────────────────────────────────
        if rm_type == "DV_DATE_TIME":
            result["value"] = "2022-02-03T04:05:06"
            return result

        # ──────────────────────────────────────────────────────────────
        # DV_INTERVAL<…>
        # ──────────────────────────────────────────────────────────────
        if rm_type.startswith("DV_INTERVAL"):
            # Render as an interval of date-times (same style as EHRbase example)
            result["_type"] = "DV_INTERVAL"      # canonical name
            result.update({
                "lower_unbounded": False,
                "upper_unbounded": False,
                "lower_included": False,
                "upper_included": False,
                "lower": {"_type": "DV_DATE_TIME", "value": "2022-02-03T04:05:06"},
                "upper": {"_type": "DV_DATE_TIME", "value": "2022-02-03T04:05:06"}
            })
            return result

        # fallback – nothing recognised
        return None
    
    def _process_code_phrase_constraint(self, constraint_node: ET.Element) -> Dict:
        """Process C_CODE_PHRASE constraint"""
        terminology = constraint_node.findtext("opt:terminology_id/opt:value", "local", self.NS)
        codes = []
        
        # Check for reference_set_uri for external terminologies
        ref_uri = constraint_node.findtext("opt:referenceSetUri", "", self.NS)
        if ref_uri:
            terminology = ref_uri
        
        for code_elem in constraint_node.findall("opt:code_list", self.NS):
            if code_elem.text:
                codes.append(code_elem.text.strip())
        
        if codes:
            code = random.choice(codes)
            return {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": terminology},
                "code_string": code
            }
        
        # Check if this is a C_CODE_REFERENCE (external terminology)
        if constraint_node.get(f"{{{self.NS['xsi']}}}type") == "C_CODE_REFERENCE":
            ref_uri = constraint_node.findtext("opt:referenceSetUri", "", self.NS)
            if ref_uri:
                terminology = ref_uri
            return {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": terminology},
                "code_string": "42"
            }
        
        # Default if no codes found
        return {
            "_type": "CODE_PHRASE",
            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": terminology},
            "code_string": "at0000"
        }
    
    def _process_primitive_constraint(self, constraint_node: ET.Element, constraint_type: str) -> Any:
        """Process primitive type constraints"""
        if constraint_type == "C_STRING":
            # Check for pattern
            pattern = constraint_node.get("pattern")
            if pattern:
                return f"text_matching_{pattern}"
            
            # Check for list of allowed values
            values = []
            for item in constraint_node.findall("opt:list", self.NS):
                if item.text:
                    values.append(item.text)
            
            return random.choice(values) if values else "Lorem ipsum"
            
        elif constraint_type == "C_INTEGER":
            range_elem = constraint_node.find("opt:range", self.NS)
            if range_elem is not None:
                lower = int(range_elem.findtext("opt:lower", "0", self.NS))
                upper = int(range_elem.findtext("opt:upper", "100", self.NS))
                return random.randint(lower, upper)
            return 50
            
        elif constraint_type == "C_REAL":
            range_elem = constraint_node.find("opt:range", self.NS)
            if range_elem is not None:
                lower = float(range_elem.findtext("opt:lower", "0.0", self.NS))
                upper = float(range_elem.findtext("opt:upper", "100.0", self.NS))
                return round(random.uniform(lower, upper), 2)
            return 1.0
            
        elif constraint_type == "C_BOOLEAN":
            true_valid = constraint_node.findtext("opt:true_valid", "true", self.NS).lower() == "true"
            false_valid = constraint_node.findtext("opt:false_valid", "true", self.NS).lower() == "true"
            
            if true_valid and not false_valid:
                return True
            elif false_valid and not true_valid:
                return False
            else:
                return random.choice([True, False])
                
        elif constraint_type == "C_DATE_TIME":
            return "2022-02-03T04:05:06"
        
        return None
    
    def _rm_type_has_name(self, rm_type: str) -> bool:
        """Check if this RM type should have a name attribute"""
        return rm_type in [
            "COMPOSITION", "SECTION", "ADMIN_ENTRY", "OBSERVATION", "EVALUATION",
            "INSTRUCTION", "ACTION", "CLUSTER", "ELEMENT", "ITEM_TREE",
            "ITEM_LIST", "ITEM_TABLE", "ITEM_SINGLE", "EVENT", "POINT_EVENT",
            "INTERVAL_EVENT", "EVENT_CONTEXT", "HISTORY", "ACTIVITY"
        ]
    
    def _add_required_rm_fields(self, result: Dict, rm_type: str):
        """Inject mandatory RM attributes that compositions expect.

        Rules:
        • CARE_ENTRY = {OBSERVATION, EVALUATION, INSTRUCTION, ACTION, ADMIN_ENTRY}
        • EVENT_CONTEXT gets start/end/setting etc.
        • ITEM_TREE/CLUSTER etc. only need an empty items list.
        """

        care_entry = [
            "ADMIN_ENTRY", "OBSERVATION", "EVALUATION",
            "INSTRUCTION", "ACTION"
        ]

        if rm_type in care_entry:
            # language & encoding -------------------------------------------------
            result.setdefault("language", {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
                "code_string": "en"
            })
            result.setdefault("encoding", {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"},
                "code_string": "ISO-10646-UTF-1"
            })
            # subject / provider --------------------------------------------------
            result.setdefault("subject", {"_type": "PARTY_SELF"})
            result.setdefault("provider", {"_type": "PARTY_SELF"})
            # workflow_id ---------------------------------------------------------
            result.setdefault("workflow_id", {
                "_type": "OBJECT_REF",
                "namespace": "unknown",
                "type": "ANY",
                "id": {
                    "_type": "GENERIC_ID",
                    "value": str(uuid.uuid4()),
                    "scheme": "scheme"
                }
            })
            # guideline_id is only required for EVALUATION ------------------------
            if rm_type == "EVALUATION":
                result.setdefault("guideline_id", {
                    "_type": "OBJECT_REF",
                    "namespace": "unknown",
                    "type": "ANY",
                    "id": {
                        "_type": "GENERIC_ID",
                        "value": str(uuid.uuid4()),
                        "scheme": "scheme"
                    }
                })

        elif rm_type == "EVENT_CONTEXT":
            result.setdefault("start_time", {
                "_type": "DV_DATE_TIME",
                "value": "2022-02-03T04:05:06"
            })
            result.setdefault("end_time", {
                "_type": "DV_DATE_TIME",
                "value": "2022-02-03T04:05:06"
            })
            result.setdefault("setting", {
                "_type": "DV_CODED_TEXT",
                "value": "home",
                "defining_code": {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                    "code_string": "225"
                }
            })
            result.setdefault("health_care_facility", {
                "_type": "PARTY_IDENTIFIED",
                "name": "DOE, John"
            })

        elif rm_type in ["ITEM_TREE", "ITEM_LIST", "CLUSTER"]:
            result.setdefault("items", [])
    
    # -----------------------------------------------------------------
    # recurse through dict / list and set primitives to None
    # -----------------------------------------------------------------
    def _nullify_leaves(self, node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                # keep meta attributes (type, name, …) intact
                if k in ("_type", "name", "archetype_details",
                         "archetype_node_id", "template_id"):
                    continue
                if isinstance(v, (dict, list)):
                    self._nullify_leaves(v)
                else:
                    node[k] = None
        elif isinstance(node, list):
            for item in node:
                self._nullify_leaves(item)

    def _is_empty_primitive(self, v: Any) -> bool:
        """
        Return True when *v* is effectively “no data”.

        Handles bare primitives **and** DV_* / CODE_PHRASE shells that
        have no   .value   *and* no   .code_string.
        """
        if v in (None, "", []):
            return True

        if isinstance(v, dict):
            # ── data-value objects (DV_TEXT, DV_CODED_TEXT, …) ──────────
            if v.get("_type", "").startswith("DV_"):
                # no .value → treat as empty
                if v.get("value") in (None, "", []):
                    # DV_CODED_TEXT often contains a defining_code;
                    # still empty if the code itself is missing
                    if v.get("defining_code", {}).get("code_string") in (None, "", []):
                        return True
                    return True          # generic DV_* with no value

            # ── CODE_PHRASE objects ─────────────────────────────────────
            if v.get("_type") == "CODE_PHRASE":
                if v.get("code_string") in (None, "", []):
                    return True

        return False

    def _prune_empty(self, node: Any):
        """
        Recursively remove

        • primitives that are None / "" / []
        • ELEMENTs whose 'value' is empty or missing
        • CLUSTER / ITEM_* whose 'items' list becomes empty
        """
        if isinstance(node, dict):

            # first descend into children ---------------------------------
            for k in list(node.keys()):
                v = node[k]
                if isinstance(v, (dict, list)):
                    self._prune_empty(v)

            # -------------------------------------------------------------
            # ① prune empty ELEMENT shells
            # -------------------------------------------------------------
            if node.get("_type") == "ELEMENT":
                if "value" not in node or self._is_empty_primitive(node["value"]):
                    # keep meta info only useful to parent for identification
                    node.clear()          # will be deleted by parent sweep

            # -------------------------------------------------------------
            # ② prune container nodes with no payload
            #    (CLUSTER, ITEM_TREE, ITEM_LIST, ITEM_TABLE, ITEM_SINGLE)
            # -------------------------------------------------------------
            if node.get("_type") in {"CLUSTER",
                                     "ITEM_TREE",
                                     "ITEM_LIST",
                                     "ITEM_TABLE",
                                     "ITEM_SINGLE"}:
                items = node.get("items", [])
                if not items:             # already empty *or* became empty
                    node.clear()

            # finally remove keys that are now {}, [] or empty primitives
            for k, v in list(node.items()):
                if v in ({}, [], None, ""):
                    node.pop(k)

        elif isinstance(node, list):
            for item in list(node):
                self._prune_empty(item)
            # drop {} / [] placeholders created above
            node[:] = [i for i in node if i not in ({}, [])]
        

    def _apply_postprocessing(self, steps: list[dict], comp: dict) -> None:
        """
        Execute YAML  _postprocessing  rules of type 'delete'.

        The rules in tumour_mapping.yaml all delete ELEMENTs that have no
        textual payload.  We detect those generically instead of parsing
        XPath.
        """

        # fast exit – nothing to do
        if not steps:
            return

        def _is_empty_value(v: Any) -> bool:
            """True if v is None / "" or a DV_* object whose own .value is empty."""
            if v in (None, "", []):
                return True
            if isinstance(v, dict) and v.get("_type", "").startswith("DV_"):
                # treat CODE_PHRASE / DV_CODED_TEXT with only defining_code as empty
                inner = v.get("value")
                return inner in (None, "", [])
            return False

        def _prune_empty_elements(node: Any) -> None:
            """Recursively delete ELEMENTs whose value is empty."""
            if isinstance(node, list):
                for item in list(node):
                    _prune_empty_elements(item)
                node[:] = [i for i in node if i not in ({}, [])]
            elif isinstance(node, dict):
                # first recurse into children
                for k in list(node.keys()):
                    _prune_empty_elements(node[k])

                # now decide if *this* dict is an empty ELEMENT
                if node.get("_type") == "ELEMENT":
                    val = node.get("value")
                    if _is_empty_value(val):
                        node.clear()            # mark for deletion in parent

                # drop keys wiped by the rule above
                for k, v in list(node.items()):
                    if v in ({}, []):
                        node.pop(k)

        # run once – covers all three YAML delete rules
        _prune_empty_elements(comp)
        
    # ──────────────────────────────────────────────────────────────
    # helper to create a bare-bones composition (all leaves = None)
    # ──────────────────────────────────────────────────────────────
    def _build_structure_from_template(self) -> Dict:
        """
        Walk the OPT once and create the JSON skeleton without filling
        any data values.  It simply re-uses _process_template_node()
        and then nullifies every primitive leaf.
        """
        root_def = self.tree.find(".//opt:definition", self.NS)
        if root_def is None:
            raise ValueError("No <definition> section in OPT")

        # 1. build full structure (with the dummy values that
        #    _process_template_node() inserts)
        structure = self._process_template_node(root_def, "/", 0)

        # 2. blank out all primitive leaves so the mapper
        #    can see which paths still need values
        self._nullify_leaves(structure)
        return structure
    
    def generate_minimal(self) -> Dict:
        """Template-conformant composition with all leaves set to None"""
        return self._build_structure_from_template()

    def generate_from_mapping(self, mapping: Dict, source: Path) -> Dict:
        composition   = self.generate_minimal()
        handler       = self._find_handler(source)
        source_tree   = handler.load_source(source)
        mapping_proc  = handler.preprocess_mapping(mapping, source_tree)

        composition   = apply_mapping(self, mapping_proc, handler, source_tree,
                                    composition)

        # existing prune_empty still removes leftover None / "" primitives
        opts = mapping.get("_options", {})
        if opts.get("prune_empty") or opts.get("prune_empty_elements"):
            self._prune_empty(composition)

        return composition

    def _find_handler(self, source_path: Path) -> SourceHandler:
        """
        Look through self.handlers (populated via .register_handler())
        and return the first one that advertises it can deal with the
        file extension of *source_path*.
        """
        for h in self.handlers:
            if h.can_handle(source_path):
                return h
        raise ValueError(f"No handler registered for '{source_path.suffix}'")
    
    # ──────────────────────────────────────────────────────────────
    # utility: depth-first walk yielding (json_path, value) pairs
    # ──────────────────────────────────────────────────────────────
    def iter_leaves(self, node: Any, path: str = ""):
        if isinstance(node, dict):
            for k, v in node.items():
                new_p = f"{path}/{k}"
                if isinstance(v, (dict, list)):
                    yield from self.iter_leaves(v, new_p)
                else:
                    yield new_p, v
        elif isinstance(node, list):
            for i, item in enumerate(node):
                yield from self.iter_leaves(item, f"{path}/{i}")
    
    def _set_value_at_path(self, obj: Dict, path: str, value: Any):
        """Set a value in the object at the given JSON path"""
        parts = path.strip('/').split('/')
        current = obj
        
        for i, part in enumerate(parts[:-1]):
            if part.isdigit():
                part = int(part)
                
            if isinstance(current, list):
                while len(current) <= part:
                    current.append({})
                current = current[part]
            else:
                if part not in current:
                    next_part = parts[i + 1]
                    if next_part.isdigit():
                        current[part] = []
                    else:
                        current[part] = {}
                current = current[part]
        
        final_key = parts[-1]
        if final_key.isdigit() and isinstance(current, list):
            final_key = int(final_key)
            while len(current) <= final_key:
                current.append({})
            current[final_key] = value
        else:
            current[final_key] = value
    
    def validate_composition(self, composition: Dict) -> List[str]:
        """Validate a composition against template constraints"""
        errors = []
        # Basic validation - could be expanded
        if not isinstance(composition, dict):
            errors.append("Composition must be a dictionary")
        elif composition.get("_type") != "COMPOSITION":
            errors.append("Root element must be a COMPOSITION")
        
        return errors

    def trace(self, mapping: Dict[str, Any], source_path: Path) -> List[Dict[str, str]]:
        from mapper.utils.trace_mapping import build_trace_table
        return build_trace_table(mapping, source_path, self.handlers)