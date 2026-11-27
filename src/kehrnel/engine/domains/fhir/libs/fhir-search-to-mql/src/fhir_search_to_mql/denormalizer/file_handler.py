"""
File handler for FHIR resource denormalization.

Provides utilities for reading, writing, and processing FHIR resources from files.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from fhir_search_to_mql.core.exceptions import DenormalizationError


class FileHandler:
    """Handle file I/O operations for FHIR resources."""
    
    @staticmethod
    def read_resource(file_path: str) -> Dict[str, Any]:
        """
        Read a FHIR resource from a JSON file.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            FHIR resource dictionary
            
        Raises:
            DenormalizationError: If file cannot be read or parsed
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                resource = json.load(f)
            
            if not isinstance(resource, dict):
                raise DenormalizationError(f"Invalid resource in {file_path}: expected dict, got {type(resource)}")
            
            return resource
            
        except FileNotFoundError:
            raise DenormalizationError(f"File not found: {file_path}")
        except json.JSONDecodeError as e:
            raise DenormalizationError(f"Invalid JSON in {file_path}: {str(e)}")
        except Exception as e:
            raise DenormalizationError(f"Error reading {file_path}: {str(e)}")
    
    @staticmethod
    def write_resource(file_path: str, resource: Dict[str, Any], indent: int = 2) -> None:
        """
        Write a FHIR resource to a JSON file.
        
        Args:
            file_path: Path to output JSON file
            resource: FHIR resource dictionary
            indent: JSON indentation (default: 2)
            
        Raises:
            DenormalizationError: If file cannot be written
        """
        try:
            # Ensure parent directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(resource, f, indent=indent, ensure_ascii=False)
                
        except Exception as e:
            raise DenormalizationError(f"Error writing {file_path}: {str(e)}")
    
    @staticmethod
    def read_bundle(file_path: str) -> List[Dict[str, Any]]:
        """
        Read a FHIR Bundle and extract all resources.
        
        Args:
            file_path: Path to Bundle JSON file
            
        Returns:
            List of FHIR resources from bundle
            
        Raises:
            DenormalizationError: If bundle cannot be read or parsed
        """
        bundle = FileHandler.read_resource(file_path)
        
        if bundle.get('resourceType') != 'Bundle':
            raise DenormalizationError(f"Resource is not a Bundle: {file_path}")
        
        resources = []
        entries = bundle.get('entry', [])
        
        for entry in entries:
            if 'resource' in entry:
                resources.append(entry['resource'])
        
        return resources
    
    @staticmethod
    def write_bundle(
        file_path: str,
        resources: List[Dict[str, Any]],
        bundle_type: str = "collection"
    ) -> None:
        """
        Write resources to a FHIR Bundle file.
        
        Args:
            file_path: Path to output Bundle JSON file
            resources: List of FHIR resources
            bundle_type: Type of bundle (default: "collection")
            
        Raises:
            DenormalizationError: If bundle cannot be written
        """
        bundle = {
            "resourceType": "Bundle",
            "type": bundle_type,
            "total": len(resources),
            "entry": [{"resource": resource} for resource in resources]
        }
        
        FileHandler.write_resource(file_path, bundle)
    
    @staticmethod
    def process_folder(
        folder_path: str,
        pattern: str = "*.json",
        recursive: bool = False,
        resource_type: Optional[str] = None,
        processor: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Process all FHIR resources from a folder.
        
        Args:
            folder_path: Path to folder containing JSON files
            pattern: File pattern (default: *.json)
            recursive: Search subdirectories (default: False)
            resource_type: Optional filter by resource type
            processor: Optional function to process each resource
            
        Returns:
            List of processed resources
            
        Raises:
            DenormalizationError: If folder cannot be accessed
        """
        path = Path(folder_path)
        
        if not path.exists():
            raise DenormalizationError(f"Folder not found: {folder_path}")
        
        if not path.is_dir():
            raise DenormalizationError(f"Not a directory: {folder_path}")
        
        # Find all matching files
        if recursive:
            files = list(path.rglob(pattern))
        else:
            files = list(path.glob(pattern))
        
        results = []
        
        for file_path in files:
            try:
                resource = FileHandler.read_resource(str(file_path))
                
                # Filter by resource type if specified
                if resource_type and resource.get('resourceType') != resource_type:
                    continue
                
                # Process if processor provided
                if processor:
                    resource = processor(resource)
                
                results.append(resource)
                
            except Exception as e:
                print(f"Warning: Failed to process {file_path}: {str(e)}")
                continue
        
        return results
    
    @staticmethod
    def batch_write(
        resources: List[Dict[str, Any]],
        output_dir: str,
        filename_template: str = "{resourceType}_{id}.json"
    ) -> int:
        """
        Write multiple resources to separate files.
        
        Args:
            resources: List of FHIR resources
            output_dir: Output directory
            filename_template: Template for filenames (default: "{resourceType}_{id}.json")
                               Can use {resourceType}, {id}, {index} placeholders
            
        Returns:
            Number of files written
            
        Raises:
            DenormalizationError: If files cannot be written
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        written = 0
        
        for index, resource in enumerate(resources):
            try:
                # Generate filename
                filename = filename_template.format(
                    resourceType=resource.get('resourceType', 'Unknown'),
                    id=resource.get('id', f'resource_{index}'),
                    index=index
                )
                
                file_path = output_path / filename
                FileHandler.write_resource(str(file_path), resource)
                written += 1
                
            except Exception as e:
                print(f"Warning: Failed to write resource {index}: {str(e)}")
                continue
        
        return written
    
    @staticmethod
    def validate_json(file_path: str) -> bool:
        """
        Validate that a file contains valid JSON.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            True if valid, False otherwise
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json.load(f)
            return True
        except Exception:
            return False
    
    @staticmethod
    def get_file_stats(folder_path: str, pattern: str = "*.json") -> Dict[str, Any]:
        """
        Get statistics about JSON files in a folder.
        
        Args:
            folder_path: Path to folder
            pattern: File pattern (default: *.json)
            
        Returns:
            Dictionary with statistics (total_files, total_size, resource_types)
        """
        path = Path(folder_path)
        
        if not path.exists() or not path.is_dir():
            return {
                'total_files': 0,
                'total_size': 0,
                'resource_types': {}
            }
        
        files = list(path.glob(pattern))
        total_size = 0
        resource_types = {}
        
        for file_path in files:
            try:
                total_size += file_path.stat().st_size
                
                # Try to read resource type
                resource = FileHandler.read_resource(str(file_path))
                res_type = resource.get('resourceType', 'Unknown')
                resource_types[res_type] = resource_types.get(res_type, 0) + 1
                
            except Exception:
                continue
        
        return {
            'total_files': len(files),
            'total_size': total_size,
            'resource_types': resource_types
        }
