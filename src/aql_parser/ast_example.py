# src/aql_parser/ast_example.py

ast_data = {
  "select": {
    "distinct": False,
    "columns": {
      "0": {
        "value": {
          "type": "dataMatchPath",
          "path": "c/name/value"
        }
      }
    }
  },
  "from": {
    "rmType": "EHR",
    "alias": "e",
    "predicate": None
  },
  "contains": {
    "rmType": "COMPOSITION",
    "alias": "c"
  },
  "limit": 5
}