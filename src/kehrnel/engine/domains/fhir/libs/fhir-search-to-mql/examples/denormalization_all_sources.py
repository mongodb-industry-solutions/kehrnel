"""
Example: Denormalize FHIR resources from various input sources.

Demonstrates all input source options from Prompt 2.1:
1. In-Memory: Pass resource dict directly
2. Single File: Load and denormalize from JSON file
3. Folder/Batch: Process multiple files
4. MongoDB Collection: Process from database
"""

from fhir_search_to_mql.denormalizer import ResourceDenormalizer, FileHandler, MongoDBHandler


# ============================================================================
# 1. IN-MEMORY: Pass Resource Dict Directly
# ============================================================================
def example_in_memory_denormalization():
    """Denormalize a resource dict in memory."""
    print("\n" + "="*80)
    print("EXAMPLE 1: IN-MEMORY DENORMALIZATION")
    print("="*80)
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Create a Patient resource in memory
    patient = {
        "resourceType": "Patient",
        "id": "example-patient",
        "name": [
            {
                "use": "official",
                "family": "Smith",
                "given": ["John", "Michael"]
            }
        ],
        "gender": "male",
        "birthDate": "1980-01-15",
        "identifier": [
            {
                "system": "http://hospital.org/mrn",
                "value": "MRN-12345"
            }
        ],
        "telecom": [
            {"system": "phone", "value": "555-1234"},
            {"system": "email", "value": "john.smith@example.com"}
        ]
    }
    
    # Denormalize
    denormalized = denormalizer.denormalize(patient)
    
    print("\nOriginal Patient:")
    print(f"  ID: {patient['id']}")
    print(f"  Name: {patient['name'][0]['given'][0]} {patient['name'][0]['family']}")
    
    print("\nDenormalized _search fields:")
    if '_search' in denormalized:
        for key, value in denormalized['_search'].items():
            print(f"  {key}: {value}")
    
    return denormalized


# ============================================================================
# 2. SINGLE FILE: Load and Denormalize from JSON File
# ============================================================================
def example_single_file_denormalization():
    """Denormalize a resource from a JSON file."""
    print("\n" + "="*80)
    print("EXAMPLE 2: SINGLE FILE DENORMALIZATION")
    print("="*80)
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Path to input file
    input_file = "data/patient_example.json"
    
    # Denormalize from file
    try:
        denormalized = denormalizer.denormalize_from_file(input_file)
        
        print(f"\nLoaded and denormalized: {input_file}")
        print(f"Resource Type: {denormalized.get('resourceType')}")
        print(f"Resource ID: {denormalized.get('id')}")
        
        if '_search' in denormalized:
            print(f"Denormalized fields: {len(denormalized['_search'])} fields")
        
        # Optionally write to output file
        output_file = "output/patient_denormalized.json"
        FileHandler.write_resource(output_file, denormalized)
        print(f"\nSaved to: {output_file}")
        
        return denormalized
    
    except Exception as e:
        print(f"Error: {e}")
        return None


# ============================================================================
# 3. FOLDER/BATCH: Process Multiple Files
# ============================================================================
def example_folder_batch_denormalization():
    """Denormalize all resources from a folder."""
    print("\n" + "="*80)
    print("EXAMPLE 3: FOLDER/BATCH DENORMALIZATION")
    print("="*80)
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Input folder
    input_folder = "data/patients"
    
    # Options
    resource_type = "Patient"  # Filter by resource type
    pattern = "*.json"         # File pattern
    recursive = False          # Search subdirectories
    
    # Denormalize all files
    try:
        denormalized_resources = denormalizer.denormalize_from_folder(
            folder_path=input_folder,
            resource_type=resource_type,
            pattern=pattern,
            recursive=recursive
        )
        
        print(f"\nProcessed folder: {input_folder}")
        print(f"Total resources denormalized: {len(denormalized_resources)}")
        
        # Show summary
        for i, resource in enumerate(denormalized_resources, 1):
            print(f"\n  {i}. {resource.get('resourceType')} / {resource.get('id')}")
            if '_search' in resource:
                print(f"     Denormalized fields: {', '.join(resource['_search'].keys())}")
        
        # Optionally save as bundle
        output_bundle = "output/patients_bundle.json"
        FileHandler.write_bundle(output_bundle, denormalized_resources)
        print(f"\nSaved bundle to: {output_bundle}")
        
        return denormalized_resources
    
    except Exception as e:
        print(f"Error: {e}")
        return []


# ============================================================================
# 4. MONGODB COLLECTION: Process from Database
# ============================================================================
def example_mongodb_denormalization():
    """Denormalize resources from a MongoDB collection."""
    print("\n" + "="*80)
    print("EXAMPLE 4: MONGODB COLLECTION DENORMALIZATION")
    print("="*80)
    
    try:
        from pymongo import MongoClient
    except ImportError:
        print("Error: pymongo not installed. Install with: pip install pymongo")
        return []
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Connect to MongoDB
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["fhir_synthetic"]
        collection = db["patients"]
        
        print(f"\nConnected to MongoDB: {client.address}")
        print(f"Database: {db.name}")
        print(f"Collection: {collection.name}")
        
        # Option A: Return denormalized resources (update_in_place=False)
        print("\n--- Option A: Return Denormalized Resources ---")
        
        query = {"resourceType": "Patient"}  # Optional filter
        batch_size = 100
        
        denormalized_resources = denormalizer.denormalize_from_mongodb(
            collection=collection,
            query=query,
            batch_size=batch_size,
            update_in_place=False  # Return results, don't update DB
        )
        
        print(f"Denormalized {len(denormalized_resources)} resources")
        
        # Option B: Update documents in place (update_in_place=True)
        print("\n--- Option B: Update Documents In Place ---")
        
        result = denormalizer.denormalize_from_mongodb(
            collection=collection,
            query=query,
            batch_size=batch_size,
            update_in_place=True  # Update _search fields in database
        )
        
        print("Documents updated in database")
        
        # Get statistics
        stats = MongoDBHandler.get_collection_stats(collection)
        print(f"\nCollection Statistics:")
        print(f"  Total documents: {stats['total_count']}")
        print(f"  With _search field: {stats['with_search']}")
        print(f"  Without _search field: {stats['without_search']}")
        print(f"  Resource types: {stats['resource_types']}")
        
        return denormalized_resources
    
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        print("Make sure MongoDB is running and accessible")
        return []


# ============================================================================
# ADVANCED: Denormalize Specific Field
# ============================================================================
def example_denormalize_specific_field():
    """Denormalize a specific field value."""
    print("\n" + "="*80)
    print("ADVANCED EXAMPLE: DENORMALIZE SPECIFIC FIELD")
    print("="*80)
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Example: Denormalize just a name field
    name_value = {
        "use": "official",
        "family": "Johnson",
        "given": ["Mary", "Jane"]
    }
    
    denormalized = denormalizer.denormalize_field(
        field_path="name",
        value=name_value,
        resource_type="Patient"
    )
    
    print("\nOriginal name value:")
    print(f"  {name_value}")
    
    print("\nDenormalized fields:")
    for key, value in denormalized.items():
        print(f"  {key}: {value}")
    
    return denormalized


# ============================================================================
# VALIDATION: Validate Denormalized Resource
# ============================================================================
def example_validate_denormalized_resource():
    """Validate a denormalized resource."""
    print("\n" + "="*80)
    print("VALIDATION EXAMPLE: VALIDATE DENORMALIZED RESOURCE")
    print("="*80)
    
    # Initialize denormalizer
    denormalizer = ResourceDenormalizer(config_dir="configs")
    
    # Create and denormalize a patient
    patient = {
        "resourceType": "Patient",
        "id": "validation-test",
        "name": [{"family": "Test", "given": ["Validation"]}]
    }
    
    denormalized = denormalizer.denormalize(patient)
    
    # Validate
    try:
        is_valid = denormalizer.validate(denormalized)
        print(f"\nValidation result: {'PASS' if is_valid else 'FAIL'}")
        print("Resource is properly denormalized")
        
    except Exception as e:
        print(f"\nValidation failed: {e}")
    
    return is_valid


# ============================================================================
# Main: Run All Examples
# ============================================================================
if __name__ == "__main__":
    print("\n" + "="*80)
    print("FHIR RESOURCE DENORMALIZATION - ALL INPUT SOURCES")
    print("="*80)
    
    # Run examples
    example_in_memory_denormalization()
    
    # Uncomment to run other examples (requires data files / MongoDB):
    # example_single_file_denormalization()
    # example_folder_batch_denormalization()
    # example_mongodb_denormalization()
    
    example_denormalize_specific_field()
    example_validate_denormalized_resource()
    
    print("\n" + "="*80)
    print("All examples completed!")
    print("="*80)
