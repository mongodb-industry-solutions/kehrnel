"""
Unit tests for file and MongoDB handlers (Phase 2).
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

from fhir_search_to_mql.denormalizer.file_handler import FileHandler
from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler
from fhir_search_to_mql.core.exceptions import DenormalizationError


class TestFileHandler:
    """Test FileHandler functionality."""
    
    def test_read_resource(self):
        """Test reading a FHIR resource from file."""
        # Create temporary file with FHIR resource
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            resource = {
                "resourceType": "Patient",
                "id": "test-123",
                "name": [{"family": "Smith", "given": ["John"]}]
            }
            json.dump(resource, f)
            temp_path = f.name
        
        try:
            # Read resource
            result = FileHandler.read_resource(temp_path)
            
            assert result["resourceType"] == "Patient"
            assert result["id"] == "test-123"
            assert len(result["name"]) == 1
        finally:
            # Clean up
            Path(temp_path).unlink()
    
    def test_read_resource_file_not_found(self):
        """Test reading non-existent file raises error."""
        with pytest.raises(DenormalizationError, match="File not found"):
            FileHandler.read_resource("/nonexistent/file.json")
    
    def test_write_resource(self):
        """Test writing a FHIR resource to file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "output" / "patient.json"
            
            resource = {
                "resourceType": "Patient",
                "id": "test-456"
            }
            
            # Write resource
            FileHandler.write_resource(str(file_path), resource)
            
            # Verify file exists and contains correct data
            assert file_path.exists()
            
            with open(file_path, 'r') as f:
                loaded = json.load(f)
            
            assert loaded["resourceType"] == "Patient"
            assert loaded["id"] == "test-456"
    
    def test_read_bundle(self):
        """Test reading a FHIR Bundle."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            bundle = {
                "resourceType": "Bundle",
                "type": "collection",
                "entry": [
                    {"resource": {"resourceType": "Patient", "id": "1"}},
                    {"resource": {"resourceType": "Patient", "id": "2"}}
                ]
            }
            json.dump(bundle, f)
            temp_path = f.name
        
        try:
            # Read bundle
            resources = FileHandler.read_bundle(temp_path)
            
            assert len(resources) == 2
            assert resources[0]["id"] == "1"
            assert resources[1]["id"] == "2"
        finally:
            Path(temp_path).unlink()
    
    def test_write_bundle(self):
        """Test writing resources to a Bundle."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "bundle.json"
            
            resources = [
                {"resourceType": "Patient", "id": "1"},
                {"resourceType": "Patient", "id": "2"}
            ]
            
            # Write bundle
            FileHandler.write_bundle(str(file_path), resources)
            
            # Verify
            with open(file_path, 'r') as f:
                bundle = json.load(f)
            
            assert bundle["resourceType"] == "Bundle"
            assert bundle["total"] == 2
            assert len(bundle["entry"]) == 2
    
    def test_process_folder(self):
        """Test processing multiple files from folder."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create multiple JSON files
            for i in range(3):
                file_path = temp_path / f"patient_{i}.json"
                resource = {"resourceType": "Patient", "id": f"pat-{i}"}
                with open(file_path, 'w') as f:
                    json.dump(resource, f)
            
            # Process folder
            results = FileHandler.process_folder(str(temp_path))
            
            assert len(results) == 3
            assert all(r["resourceType"] == "Patient" for r in results)
    
    def test_batch_write(self):
        """Test writing multiple resources to separate files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            resources = [
                {"resourceType": "Patient", "id": "1"},
                {"resourceType": "Patient", "id": "2"},
                {"resourceType": "Observation", "id": "obs-1"}
            ]
            
            # Batch write
            count = FileHandler.batch_write(resources, temp_dir)
            
            assert count == 3
            
            # Verify files exist
            assert (Path(temp_dir) / "Patient_1.json").exists()
            assert (Path(temp_dir) / "Patient_2.json").exists()
            assert (Path(temp_dir) / "Observation_obs-1.json").exists()


class TestMongoDBHandler:
    """Test MongoDBHandler functionality."""
    
    def test_read_resources(self):
        """Test reading resources from MongoDB."""
        # Mock collection
        mock_collection = Mock()
        mock_collection.find.return_value.limit.return_value = [
            {"resourceType": "Patient", "id": "1"},
            {"resourceType": "Patient", "id": "2"}
        ]
        
        # Read resources
        results = MongoDBHandler.read_resources(mock_collection, limit=2)
        
        assert len(results) == 2
        mock_collection.find.assert_called_once()
    
    def test_write_resources(self):
        """Test writing resources to MongoDB."""
        # Mock collection
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.inserted_ids = ["id1", "id2"]
        mock_collection.insert_many.return_value = mock_result
        
        resources = [
            {"resourceType": "Patient", "id": "1"},
            {"resourceType": "Patient", "id": "2"}
        ]
        
        # Write resources
        count = MongoDBHandler.write_resources(mock_collection, resources)
        
        assert count == 2
        mock_collection.insert_many.assert_called_once()
    
    def test_update_search_fields(self):
        """Test updating _search fields."""
        # Mock collection
        mock_collection = Mock()
        mock_collection.count_documents.return_value = 2
        mock_collection.find.return_value.batch_size.return_value = [
            {"_id": "1", "resourceType": "Patient"},
            {"_id": "2", "resourceType": "Patient"}
        ]
        
        # Mock update result
        mock_update_result = Mock()
        mock_update_result.modified_count = 1
        mock_collection.update_one.return_value = mock_update_result
        
        # Processor function
        def processor(resource):
            return {"_search": {"processed": True}}
        
        # Update search fields
        stats = MongoDBHandler.update_search_fields(
            mock_collection,
            processor=processor
        )
        
        assert stats['processed'] == 2
        assert stats['updated'] == 2
        assert stats['failed'] == 0
    
    def test_get_collection_stats(self):
        """Test getting collection statistics."""
        # Mock collection
        mock_collection = Mock()
        mock_collection.count_documents.side_effect = [100, 75]  # total, with_search
        mock_collection.aggregate.return_value = [
            {"_id": "Patient", "count": 60},
            {"_id": "Observation", "count": 40}
        ]
        
        # Get stats
        stats = MongoDBHandler.get_collection_stats(mock_collection)
        
        assert stats['total_count'] == 100
        assert stats['with_search'] == 75
        assert stats['without_search'] == 25
        assert len(stats['resource_types']) == 2
    
    def test_ensure_indexes(self):
        """Test ensuring indexes exist."""
        # Mock collection
        mock_collection = Mock()
        mock_collection.create_index.return_value = "idx_name"
        
        indexes = [
            {
                "fields": {"_search.familyName_lower": 1},
                "options": {"name": "idx_family_name"}
            }
        ]
        
        # Create indexes
        created = MongoDBHandler.ensure_indexes(mock_collection, indexes)
        
        assert len(created) == 1
        mock_collection.create_index.assert_called_once()
    
    def test_remove_search_fields(self):
        """Test removing _search fields."""
        # Mock collection
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.modified_count = 50
        mock_collection.update_many.return_value = mock_result
        
        # Remove search fields
        count = MongoDBHandler.remove_search_fields(mock_collection)
        
        assert count == 50
        mock_collection.update_many.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
