"""
Example: Query Conversion Only

This example shows how to use the FHIRSearchConverter to convert
FHIR search queries to MongoDB Query Language (MQL).

Usage:
    python examples/query_conversion_example.py
"""

import json
from fhir_search_to_mql import FHIRSearchConverter

def main():
    print("FHIR Search to MQL Conversion Example")
    print("=" * 80)
    
    # Initialize converter
    converter = FHIRSearchConverter(config_dir="configs")
    
    # Patient searches
    print("\nPATIENT SEARCHES:")
    print("-" * 80)
    
    patient_queries = [
        "name=Smith",
        "name:exact=Smith",
        "family=Smith&given=John",
        "gender=male",
        "birthdate=ge1980-01-01",
        "birthdate=1980",
        "identifier=http://hospital.smarthealthit.org|12345",
        "address-city=Boston",
        "active=true",
    ]
    
    for query in patient_queries:
        print(f"\nFHIR: {query}")
        mql = converter.convert("Patient", query_string=query)
        print(f"MQL:  {json.dumps(mql, indent=6, default=str)}")
    
    # Observation searches
    print("\n\nOBSERVATION SEARCHES:")
    print("-" * 80)
    
    observation_queries = [
        "code=8480-6",
        "code=http://loinc.org|8480-6",
        "patient=Patient/123",
        "date=ge2024-01-01",
        "status=final",
        "code=8480-6&date=ge2024-01-01",
    ]
    
    for query in observation_queries:
        print(f"\nFHIR: {query}")
        mql = converter.convert("Observation", query_string=query)
        print(f"MQL:  {json.dumps(mql, indent=6, default=str)}")
    
    # Get supported parameters
    print("\n\nSUPPORTED PARAMETERS:")
    print("-" * 80)
    
    patient_params = converter.get_supported_parameters("Patient")
    print(f"Patient: {', '.join(patient_params)}")
    
    observation_params = converter.get_supported_parameters("Observation")
    print(f"Observation: {', '.join(observation_params)}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
