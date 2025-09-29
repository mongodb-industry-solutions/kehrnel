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
          "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string"
        },
        "alias": "Centro"
      },
      "2": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/other_participations/performer"
        },
        "alias": "Profesional"
      },
      "3": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[openEHR-EHR-CLUSTER.medication.v2]/items[at0132]/value/mappings/target/code_string"
        },
        "alias": "MarcaComercial"
      },
      "4": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[openEHR-EHR-CLUSTER.medication.v2]/items[at0150]/value"
        },
        "alias": "CodiNacional"
      },
      "5": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/time"
        },
        "alias": "FechaAdmin"
      },
      "6": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/ism_transition/current_state/value"
        },
        "alias": "Estado"
      },
      "7": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[at0020]"
        },
        "alias": "NombreGenerico"
      },
      "8": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[openEHR-EHR-CLUSTER.medication.v2]/items[at0150]"
        },
        "alias": "Lote"
      },
      "9": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[openEHR-EHR-CLUSTER.medication.v2]/items[at0003]"
        },
        "alias": "Caducidad"
      },
      "10": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[at0140]/items[at0147]"
        },
        "alias": "ViaAdmin"
      },
      "11": {
        "value": {
          "type": "dataMatchPath",
          "path": "med_ac/description[at0017]/items[at0140]/items[at0141]"
        },
        "alias": "LocalizAdmin"
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
    "alias": "c",
    "predicate": {
      "path": "archetype_node_id",
      "operator": "=",
      "value": "openEHR-EHR-COMPOSITION.vaccination_list.v0"
    },
    "contains": {
      "operator": "AND",
      "children": {
        "0": {
          "rmType": "CLUSTER",
          "alias": "admin_salut",
          "predicate": {
            "path": "archetype_node_id",
            "operator": "=",
            "value": "openEHR-EHR-CLUSTER.admin_salut.v0"
          }
        },
        "1": {
          "rmType": "SECTION",
          "alias": "",
          "predicate": {
            "path": "archetype_node_id",
            "operator": "=",
            "value": "openEHR-EHR-SECTION.immunisation_list.v0"
          },
          "contains": {
            "rmType": "ACTION",
            "alias": "med_ac",
            "predicate": {
              "path": "archetype_node_id",
              "operator": "=",
              "value": "openEHR-EHR-ACTION.medication.v1"
            }
          }
        }
      }
    }
  },
  "where": {
    "operator": "AND",
    "conditions": {
      "0": {
        "path": "med_ac/time",
        "operator": ">=",
        "value": "2023-01-01T00:00:00+00:00"
      },
      "1": {
        "operator": "AND",
        "conditions": {
          "0": {
            "path": "med_ac/time",
            "operator": "<",
            "value": "2023-02-01T00:00:00+00:00"
          },
          "1": {
            "operator": "AND",
            "conditions": {
              "0": {
                "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
                "operator": "=",
                "value": "E08025213"
              },
              "1": {
                "path": "med_ac/other_participations/performer/identifiers/id",
                "operator": "=",
                "value": "30847487"
              }
            }
          }
        }
      }
    }
  },
  "orderBy": {},
  "limit": None,
  "offset": None
}