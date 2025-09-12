# src/aql_parser/ast_example.py

ast_data = {
  "select": {
    "distinct": False,
    "columns": {
      "0": {
        "value": {
          "type": "dataMatchPath",
          "path": "e/ehr_id/value"
        }
      },
      "1": {
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
  "where": {
    "path": "c/name/value",
    "operator": "=",
    "value": "HC3 Reports"
  },
  "orderBy": {},
  "limit": None,
  "offset": None
}