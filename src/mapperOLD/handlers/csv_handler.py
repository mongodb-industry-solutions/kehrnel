# openEHRMapper/handlers/csv_handler_gio.py

import csv
import re
import logging
import uuid # <-- NEW: Import uuid for generating unique IDs
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from mapper.mapping_engine import SourceHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVHandler(SourceHandler):
    """
    A CSV handler that preprocesses a hierarchical mapping YAML and CSV data.
    """

    _is_preprocessor: bool = True 
    
    def can_handle(self, source_path: Path) -> bool:
        return source_path.suffix.lower() == '.csv'

    def load_source(self, source_path: Path) -> List[Dict[str, Any]]:
        try:
            with open(source_path, mode='r', encoding='utf-8-sig') as infile:
                reader = csv.DictReader(infile, delimiter=';')
                data = [row for row in reader]
                logging.info(f"Successfully loaded {len(data)} rows from {source_path}")
                return data
        except FileNotFoundError:
            logging.error(f"Source file not found at: {source_path}")
            return []
        except Exception as e:
            logging.error(f"Error loading CSV file {source_path}: {e}")
            return []

    def preprocess_mapping(self, mapping: Dict, source_data: List[Dict]) -> List[Tuple[str, List[Dict]]]:
        if not source_data:
            logging.warning("Source data is empty. No rules will be generated.")
            return []

        try:
            preprocessing_rule = mapping.get('_preprocessing', [{}])[0]
            group_by_column = preprocessing_rule.get('group_by_column')
            if not group_by_column:
                raise KeyError("'_preprocessing' directive must contain 'group_by_column'.")
        except (KeyError, IndexError):
            logging.error("A valid '_preprocessing' directive was not found in mapping.yml.")
            return []

        groups = defaultdict(list)
        for row in source_data:
            key = row.get(group_by_column)
            if key is not None:
                groups[key].append(row)
        
        logging.info(f"Data grouped by '{group_by_column}' into {len(groups)} groups.")
        
        all_composition_rules = []

        for i, (group_key, group_rows) in enumerate(groups.items()):
            if not group_rows:
                continue
            
            rules_for_this_composition = []
            first_row_in_group = group_rows[0]
            group_context = {'group_key': group_key, 'group_index': i}

            for path_template, rule_template in mapping.items():
                if path_template.startswith('_'):
                    continue

                path = path_template.format(i=i)
                
                value = self._resolve_nested_dict(
                    template=rule_template, 
                    first_row=first_row_in_group, 
                    all_rows=group_rows,
                    context=group_context
                )

                if value is not None:
                    rules_for_this_composition.append({'path': path, 'fixed_value': value})
            
            if rules_for_this_composition:
                all_composition_rules.append((group_key, rules_for_this_composition))

        logging.info(f"Preprocessing created {len(all_composition_rules)} rule sets for generation.")
        return all_composition_rules

    def _resolve_nested_dict(self, template: Any, first_row: Dict, all_rows: List[Dict], context: Dict) -> Any:
        if isinstance(template, list):
            resolved_list = []
            for item_template in template:
                if isinstance(item_template, dict) and 'for_each_row' in item_template:
                    loop_template = item_template['for_each_row']
                    for row in all_rows:
                        resolved_item = self._resolve_nested_dict(loop_template, row, all_rows, context)
                        if resolved_item is not None:
                            resolved_list.append(resolved_item)
                else:
                    resolved_item = self._resolve_nested_dict(item_template, first_row, all_rows, context)
                    if resolved_item is not None:
                        resolved_list.append(resolved_item)
            return resolved_list or None

        if isinstance(template, dict):
            rule_keys = {'column', 'map', 'transform', 'from_group', 'from_row', '_special_rule'}
            if any(key in template for key in rule_keys):
                return self._resolve_value(template, first_row, context)
            
            resolved_dict = {}
            for key, sub_template in template.items():
                clean_key = key.split(":", 1)[1] if key.startswith("constant:") else key
                resolved_value = self._resolve_nested_dict(sub_template, first_row, all_rows, context)
                if resolved_value is not None:
                    resolved_dict[clean_key] = resolved_value
            return resolved_dict or None

        return self._resolve_value(template, first_row, context)

    def _resolve_value(self, rule: Any, row: Dict, context: Dict) -> Optional[Any]:
        if isinstance(rule, str) and rule.startswith("constant:"):
            return rule.split(":", 1)[1]
        
        if not isinstance(rule, dict):
            return rule

        if '_special_rule' in rule:
            # --- MODIFIED: Added handler for the new rule ---
            if rule['_special_rule'] == 'build_analyte_value':
                return self._build_analyte_value(rule, row)
            if rule['_special_rule'] == 'generate_workflow_id':
                return self._build_workflow_id(context)
            return None

        if 'from_group' in rule:
            if rule['from_group'] == 'key':
                return context.get('group_key')
            base_value = row.get(rule.get('column'))
        elif 'from_row' in rule:
            base_value = row.get(rule.get('column'))
        else:
            base_value = row.get(rule.get('column'))

        if isinstance(base_value, str):
            base_value = base_value.strip()

        if 'map' in rule and isinstance(rule['map'], dict):
            base_value = rule['map'].get(str(base_value), base_value)

        if 'transform' in rule:
            base_value = self._apply_transform(base_value, rule['transform'])
        
        return base_value
    
    # --- NEW: Helper function to build a workflow_id OBJECT_REF ---
    def _build_workflow_id(self, context: Dict) -> Dict:
        """
        Builds a deterministic workflow_id using a UUIDv5 generated from a
        namespace and the group key.
        """
        # Create a deterministic UUID from the analysis identifier
        namespace = uuid.UUID('f81d4fae-7dec-11d0-a765-00a0c91e6bf6') # A standard namespace
        group_key = context.get('group_key', 'unknown_key')
        deterministic_uuid = str(uuid.uuid5(namespace, group_key))

        return {
            "_type": "OBJECT_REF",
            "namespace": "local_workflow",
            "type": "WORKFLOW",
            "id": {
                "_type": "HIER_OBJECT_ID", # Using HIER_OBJECT_ID which is common for UUIDs
                "value": deterministic_uuid
            }
        }

    def _build_analyte_value(self, rule: Dict, row: Dict) -> Optional[Dict]:
        """
        Builds an analyte value object (DV_QUANTITY or DV_TEXT) with robust parsing.
        It attempts to parse a numeric value even if the type is 'TX'.
        """
        result_type = row.get(rule['type_column'], '').strip()
        result_value_str = row.get(rule['result_column'], '').strip()
        units_from_col = row.get(rule['unit_column'], '').strip()

        if not result_value_str:
            return None

        # --- ★★★ NEW ROBUST PARSING LOGIC ★★★ ---
        # Attempt to parse as a quantity regardless of the type hint, as data can be messy.
        # Regex to find a numeric part (with optional <, >) and a unit part.
        quantity_match = re.match(r'^\s*([<>=\s]*[0-9.,]+)\s*(.*)\s*$', result_value_str)

        if quantity_match:
            numeric_part = quantity_match.group(1).strip()
            unit_part = quantity_match.group(2).strip()
            
            # Use the dedicated units column if it has data, otherwise use the parsed unit part.
            final_units = units_from_col if units_from_col else unit_part

            # Try to convert the numeric string to a float
            magnitude = self._apply_transform(numeric_part, 'to_number')
            
            if magnitude is not None:
                # Successfully parsed a quantity!
                return {
                    "_type": "DV_QUANTITY", 
                    "magnitude": magnitude, 
                    "units": final_units if final_units else "1" # Default to '1' if no units found
                }
        
        # --- Fallback to original logic / DV_TEXT ---
        # If it's not a parsable quantity, treat it as text.
        if result_type == "TX" or quantity_match is None:
            return {"_type": "DV_TEXT", "value": result_value_str}

        # Handle NM, ST as quantities if they weren't caught by the robust parser
        if result_type in ("NM", "ST"):
            magnitude = self._apply_transform(result_value_str, 'to_number')
            if magnitude is not None:
                return {
                    "_type": "DV_QUANTITY", 
                    "magnitude": magnitude, 
                    "units": units_from_col if units_from_col else "1"
                }

        # Final fallback if logic fails for any reason
        logging.warning(f"Could not reliably build analyte value. Falling back to DV_TEXT. Type: '{result_type}', Value: '{result_value_str}'")
        return {"_type": "DV_TEXT", "value": result_value_str}
    
    def _apply_transform(self, value: Any, transform_name: str) -> Any:
        if value is None: return None

        if transform_name == 'trim':
            return str(value).strip() if isinstance(value, (str, int, float)) else value
        
        if transform_name == 'dmy_to_iso':
            if isinstance(value, str) and re.match(r"^\d{2}/\d{2}/\d{4}$", value):
                try: return f"{value[6:10]}-{value[3:5]}-{value[0:2]}T00:00:00Z"
                except IndexError: return None
            return value
            
        if transform_name == 'to_number':
            if not isinstance(value, str): return value
            cleaned_value = re.sub(r"[^0-9,.]", "", value).replace(',', '.')
            try: return float(cleaned_value)
            except (ValueError, TypeError): return None
                
        return value

    def extract_value(self, source_data: Any, extraction_rule: Any) -> Any:
        return None

    def count_elements(self, source_data: List[Dict], path: str) -> int:
        return 0