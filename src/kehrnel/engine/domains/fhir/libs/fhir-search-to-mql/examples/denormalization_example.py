"""
Example: Denormalization Only

This example shows how to use the ResourceDenormalizer to add _search fields
to FHIR resources for optimized MongoDB querying.

Usage:
    python examples/denormalization_example.py
"""

import json
from fhir_search_to_mql import ResourceDenormalizer

# Sample Patient
patient = {
    "resourceType": "Patient",
    "id": "example",
    "name": [
        {
            "use": "official",
            "family": "Chalmers",
            "given": ["Peter", "James"]
        }
    ],
    "gender": "male",
    "birthDate": "1974-12-25",
    "active": True
}

def main():
    print("Resource Denormalization Example")
    print("=" * 80)
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Denormalize the resource
    print("\nOriginal Patient:")
    print(json.dumps(patient, indent=2))
    
    denormalized = denormalizer.denormalize(patient)
    
    print("\nDenormalized Patient:")
    print(json.dumps(denormalized, indent=2))
    
    print("\n_search fields added:")
    print(json.dumps(denormalized.get('_search', {}), indent=2))
    
    # Denormalize from file
    print("\n" + "=" * 80)
    print("Denormalize from file:")
    print("=" * 80)
    
    # Save sample to file
    with open("sample_patient.json", "w") as f:
        json.dump(patient, f, indent=2)
    
    denormalized_from_file = denormalizer.denormalize_from_file("sample_patient.json")
    print(f"Successfully denormalized from file: {denormalized_from_file.get('id')}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
