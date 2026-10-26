"""
Multi-step query support for reference chaining and reverse chaining.

This module provides classes for handling queries that require multiple steps:
- Reference chaining: subject:Patient.name=Smith
- Reverse chaining: _has:Observation:subject:code=8480-6
- :identifier modifier on references
"""

from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field


@dataclass
class QueryStep:
    """
    Represents a single step in a multi-step query.
    
    Attributes:
        resource_type: Resource type to query (e.g., "Patient")
        query: MongoDB query for this step
        extract_field: Field to extract from results (e.g., "id" or "_id")
        description: Human-readable description of this step
    """
    resource_type: str
    query: Dict[str, Any]
    extract_field: str = "id"
    extract_transform: str = "identity"
    description: str = ""


@dataclass
class MultiStepQuery:
    """
    Represents a query that requires multiple steps to execute.
    
    Multi-step queries are needed for:
    - Reference chaining (subject:Patient.name=Smith)
    - Reverse chaining (_has:Observation:subject:code=8480-6)
    - :identifier modifier on references
    
    Example:
        # Find patients whose observations have specific code
        query = MultiStepQuery(
            steps=[
                QueryStep(
                    resource_type="Observation",
                    query={"code.coding.code": "8480-6"},
                    extract_field="subject.reference",
                    description="Find observations with code 8480-6"
                )
            ],
            final_query_builder=lambda ids: {"_id": {"$in": ids}},
            description="Patients with observations having code 8480-6"
        )
    
    Attributes:
        steps: List of query steps to execute in order
        final_query_builder: Function that builds final query from extracted IDs
        description: Human-readable description of the multi-step query
        is_multi_step: Always True (for type checking)
    """
    steps: List[QueryStep] = field(default_factory=list)
    final_query_builder: Optional[Callable[[List[str]], Dict[str, Any]]] = None
    description: str = ""
    is_multi_step: bool = True
    target_field: str = "id"
    target_constraints: Dict[str, Any] = field(default_factory=dict)
    
    def add_step(
        self,
        resource_type: str,
        query: Dict[str, Any],
        extract_field: str = "id",
        extract_transform: str = "identity",
        description: str = ""
    ) -> None:
        """
        Add a query step.
        
        Args:
            resource_type: Resource type to query
            query: MongoDB query for this step
            extract_field: Field to extract from results
            description: Description of this step
        """
        step = QueryStep(
            resource_type=resource_type,
            query=query,
            extract_field=extract_field,
            extract_transform=extract_transform,
            description=description
        )
        self.steps.append(step)
    
    def set_final_query_builder(
        self,
        builder: Callable[[List[str]], Dict[str, Any]]
    ) -> None:
        """
        Set the function that builds the final query from extracted IDs.
        
        Args:
            builder: Function that takes a list of IDs and returns MongoDB query
        """
        self.final_query_builder = builder
    
    def get_execution_plan(self) -> Dict[str, Any]:
        """
        Get a summary of the execution plan.
        
        Returns:
            Dictionary describing the execution plan
        """
        return {
            "version": 1,
            "description": self.description,
            "is_multi_step": True,
            "num_steps": len(self.steps),
            "target_field": self.target_field,
            "target_constraints": self.target_constraints,
            "steps": [
                {
                    "step_number": i + 1,
                    "resource_type": step.resource_type,
                    "extract_field": step.extract_field,
                    "extract_transform": step.extract_transform,
                    "description": step.description,
                    "query": step.query
                }
                for i, step in enumerate(self.steps)
            ],
            "note": "This query requires multiple database operations and may be slower than single-step queries"
        }
    
    def to_aggregation_pipeline(self, base_collection: str) -> List[Dict[str, Any]]:
        """
        Convert multi-step query to MongoDB aggregation pipeline with $lookup.
        
        This is an alternative to executing multiple queries. It uses MongoDB's
        aggregation framework to perform joins across collections.
        
        Args:
            base_collection: Starting collection name
            
        Returns:
            List of aggregation pipeline stages
            
        Note:
            This is an advanced feature and may not be supported in all scenarios.
            Multi-step execution is the more flexible approach.
        """
        pipeline = []
        
        for i, step in enumerate(self.steps):
            # Add $lookup stage for each step
            pipeline.append({
                "$lookup": {
                    "from": step.resource_type,
                    "let": {"ref_id": f"${step.extract_field}"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$ref_id"]}}},
                        {"$match": step.query}
                    ],
                    "as": f"step_{i}_results"
                }
            })
            
            # Filter to only documents that have matching results
            pipeline.append({
                "$match": {
                    f"step_{i}_results": {"$ne": []}
                }
            })
        
        return pipeline


def is_multi_step_query(obj: Any) -> bool:
    """
    Check if an object is a MultiStepQuery.
    
    Args:
        obj: Object to check
        
    Returns:
        True if object is a MultiStepQuery
    """
    return isinstance(obj, MultiStepQuery) or (
        isinstance(obj, dict) and obj.get('is_multi_step', False)
    )


def create_simple_multi_step_query(
    resource_type: str,
    query: Dict[str, Any],
    extract_field: str,
    final_field: str,
    description: str = ""
) -> MultiStepQuery:
    """
    Create a simple two-step query (query -> extract -> final query).
    
    Args:
        resource_type: Resource type for first step
        query: Query for first step
        extract_field: Field to extract from first step results
        final_field: Field to use in final query
        description: Description of the query
        
    Returns:
        MultiStepQuery object
    """
    multi_step = MultiStepQuery(description=description)
    multi_step.target_field = final_field
    
    multi_step.add_step(
        resource_type=resource_type,
        query=query,
        extract_field=extract_field,
        description=f"Query {resource_type} and extract {extract_field}"
    )
    
    multi_step.set_final_query_builder(
        lambda ids: {final_field: {"$in": ids}} if ids else {final_field: None}
    )
    
    return multi_step
