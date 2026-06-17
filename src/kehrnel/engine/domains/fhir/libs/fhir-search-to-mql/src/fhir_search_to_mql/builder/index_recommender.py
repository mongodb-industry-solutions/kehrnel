"""
Index recommender for MongoDB queries.

Analyzes queries and recommends indexes with priorities and creation commands.
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum


class IndexPriority(Enum):
    """Index priority levels."""
    CRITICAL = "critical"  # Query will be very slow without index
    HIGH = "high"  # Significant performance improvement
    MEDIUM = "medium"  # Moderate improvement
    LOW = "low"  # Minor improvement


@dataclass
class IndexRecommendation:
    """
    Index recommendation.
    
    Attributes:
        fields: List of field names in index
        priority: Priority level
        reason: Explanation for recommendation
        command: MongoDB index creation command
        index_type: Type of index (single, compound, text, etc.)
    """
    fields: List[str]
    priority: IndexPriority
    reason: str
    command: str
    index_type: str = "single"
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"[{self.priority.value.upper()}] {self.index_type.title()} Index\n"
            f"Fields: {', '.join(self.fields)}\n"
            f"Reason: {self.reason}\n"
            f"Command: {self.command}"
        )


class IndexRecommender:
    """
    Recommend MongoDB indexes based on query analysis.
    
    Analyzes query structure and suggests optimal indexes:
    - Single-field indexes for simple queries
    - Compound indexes for multi-field queries
    - Text indexes for text search
    - Prioritizes recommendations by impact
    """
    
    def __init__(self, resource_type: str = "Resource", config: Optional[Dict[str, Any]] = None):
        """
        Initialize index recommender.
        
        Args:
            resource_type: FHIR resource type (for collection name)
            config: Resource configuration (optional)
        """
        self.resource_type = resource_type
        self.config = config or {}
        self.collection_name = resource_type
    
    def analyze(self, query: Dict[str, Any]) -> List[IndexRecommendation]:
        """
        Analyze query and generate index recommendations.
        
        Args:
            query: MongoDB query to analyze
            
        Returns:
            List of IndexRecommendation objects, sorted by priority
        """
        recommendations = []
        
        # Extract queried fields
        fields_info = self._extract_fields(query)
        
        # Generate recommendations based on field patterns
        recommendations.extend(self._recommend_single_field_indexes(fields_info))
        recommendations.extend(self._recommend_compound_indexes(fields_info))
        recommendations.extend(self._recommend_text_indexes(query))
        
        # Sort by priority (critical first)
        priority_order = {
            IndexPriority.CRITICAL: 0,
            IndexPriority.HIGH: 1,
            IndexPriority.MEDIUM: 2,
            IndexPriority.LOW: 3
        }
        recommendations.sort(key=lambda r: priority_order[r.priority])
        
        return recommendations
    
    def _extract_fields(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all queried fields from query.
        
        Returns list of field info dicts with:
        - field: Field name
        - operator: Query operator (eq, gt, regex, etc.)
        - has_range: Whether field uses range query
        
        Args:
            query: MongoDB query
            
        Returns:
            List of field information dicts
        """
        fields = []
        self._extract_fields_recursive(query, fields)
        return fields
    
    def _extract_fields_recursive(
        self,
        query: Any,
        fields: List[Dict[str, Any]],
        parent_op: Optional[str] = None
    ) -> None:
        """Recursively extract fields from query."""
        if not isinstance(query, dict):
            return
        
        for key, value in query.items():
            if key in ['$and', '$or', '$nor']:
                # Logical operator - process children
                if isinstance(value, list):
                    for item in value:
                        self._extract_fields_recursive(item, fields, key)
            elif key.startswith('$'):
                # Query operator - note it but don't add as field
                pass
            else:
                # Field name
                field_info = {
                    'field': key,
                    'operator': 'eq',
                    'has_range': False,
                    'parent_op': parent_op
                }
                
                # Analyze field value for operators
                if isinstance(value, dict):
                    for op_key in value.keys():
                        if op_key in ['$gt', '$gte', '$lt', '$lte']:
                            field_info['has_range'] = True
                            field_info['operator'] = 'range'
                        elif op_key == '$regex':
                            field_info['operator'] = 'regex'
                        elif op_key == '$text':
                            field_info['operator'] = 'text'
                        elif op_key == '$in':
                            field_info['operator'] = 'in'
                        elif op_key == '$elemMatch':
                            field_info['operator'] = 'elemMatch'
                            # Recurse into $elemMatch
                            self._extract_fields_recursive(value[op_key], fields, parent_op)
                
                fields.append(field_info)
    
    def _recommend_single_field_indexes(
        self,
        fields_info: List[Dict[str, Any]]
    ) -> List[IndexRecommendation]:
        """Recommend single-field indexes."""
        recommendations = []
        seen_fields = set()
        
        for field_info in fields_info:
            field = field_info['field']
            
            # Skip if already recommended
            if field in seen_fields:
                continue
            seen_fields.add(field)
            
            # Check if field already has index in config
            if self._has_index_in_config(field):
                continue
            
            # Determine priority
            priority = self._determine_priority(field_info)
            
            # Generate recommendation
            reason = self._generate_reason(field_info)
            command = f'db.{self.collection_name}.createIndex({{"{{field}}": 1}})'.replace('{field}', field)
            
            recommendations.append(IndexRecommendation(
                fields=[field],
                priority=priority,
                reason=reason,
                command=command,
                index_type="single"
            ))
        
        return recommendations
    
    def _recommend_compound_indexes(
        self,
        fields_info: List[Dict[str, Any]]
    ) -> List[IndexRecommendation]:
        """Recommend compound indexes for multi-field queries."""
        recommendations = []
        
        # Group fields by parent logical operator
        and_fields = [f for f in fields_info if f.get('parent_op') == '$and']
        
        if len(and_fields) >= 2:
            # Multiple fields in AND - recommend compound index
            field_names = [f['field'] for f in and_fields[:3]]  # Limit to 3 fields
            field_names = list(dict.fromkeys(field_names))  # Remove duplicates, preserve order
            
            if len(field_names) >= 2:
                # Check if compound index makes sense
                has_range = any(f['has_range'] for f in and_fields[:len(field_names)])
                
                # Build index spec
                index_spec = ', '.join([f'"{field}": 1' for field in field_names])
                command = f'db.{self.collection_name}.createIndex({{{index_spec}}})'
                
                reason = f"Compound index for {len(field_names)} fields in AND query"
                if has_range:
                    reason += " (includes range query)"
                
                priority = IndexPriority.HIGH if has_range else IndexPriority.MEDIUM
                
                recommendations.append(IndexRecommendation(
                    fields=field_names,
                    priority=priority,
                    reason=reason,
                    command=command,
                    index_type="compound"
                ))
        
        return recommendations
    
    def _recommend_text_indexes(self, query: Dict[str, Any]) -> List[IndexRecommendation]:
        """Recommend text indexes for text search."""
        recommendations = []
        
        # Check for $text operator
        if self._has_text_search(query):
            command = f'db.{self.collection_name}.createIndex({{"$**": "text"}})'
            
            recommendations.append(IndexRecommendation(
                fields=["$**"],
                priority=IndexPriority.CRITICAL,
                reason="Text search requires text index",
                command=command,
                index_type="text"
            ))
        
        # Check for $regex (suggest text index as alternative)
        regex_fields = [f['field'] for f in self._extract_fields(query) if f['operator'] == 'regex']
        
        for field in regex_fields:
            command = f'db.{self.collection_name}.createIndex({{"{{field}}": "text"}})'.replace('{field}', field)
            
            recommendations.append(IndexRecommendation(
                fields=[field],
                priority=IndexPriority.HIGH,
                reason=f"Regex query on '{field}' - text index recommended for better performance",
                command=command,
                index_type="text"
            ))
        
        return recommendations
    
    def _determine_priority(self, field_info: Dict[str, Any]) -> IndexPriority:
        """Determine index priority based on field info."""
        operator = field_info.get('operator', 'eq')
        
        # Critical: Full collection scan operations
        if operator in ['regex', 'text']:
            return IndexPriority.CRITICAL
        
        # High: Range queries
        if operator == 'range':
            return IndexPriority.HIGH
        
        # High: Frequently queried fields
        field = field_info['field']
        if any(keyword in field.lower() for keyword in ['id', '_id', 'identifier']):
            return IndexPriority.HIGH
        
        # Medium: OR queries
        if field_info.get('parent_op') == '$or':
            return IndexPriority.MEDIUM
        
        # Default: Medium
        return IndexPriority.MEDIUM
    
    def _generate_reason(self, field_info: Dict[str, Any]) -> str:
        """Generate reason for index recommendation."""
        field = field_info['field']
        operator = field_info.get('operator', 'eq')
        
        if operator == 'regex':
            return f"Regex query on '{field}' is very slow without index"
        elif operator == 'range':
            return f"Range query on '{field}' benefits from index"
        elif operator == 'in':
            return f"$in query on '{field}' benefits from index"
        elif operator == 'elemMatch':
            return f"Array query on '{field}' benefits from index"
        elif 'id' in field.lower():
            return f"ID field '{field}' should be indexed for fast lookups"
        else:
            return f"Equality query on '{field}' benefits from index"
    
    def _has_index_in_config(self, field: str) -> bool:
        """Check if field has index hint in configuration."""
        # Check all parameters for this field
        for param_name, param_config in self.config.items():
            fields = param_config.get('fields', [])
            for field_config in fields:
                if field_config.get('field') == field:
                    if field_config.get('indexed', False):
                        return True
        
        return False
    
    def _has_text_search(self, query: Dict[str, Any]) -> bool:
        """Check if query uses $text operator."""
        if not isinstance(query, dict):
            return False
        
        if '$text' in query:
            return True
        
        # Recursively check
        for value in query.values():
            if isinstance(value, dict):
                if self._has_text_search(value):
                    return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and self._has_text_search(item):
                        return True
        
        return False
    
    def format_recommendations(
        self,
        recommendations: List[IndexRecommendation],
        format: str = "text"
    ) -> str:
        """
        Format recommendations for display.
        
        Args:
            recommendations: List of recommendations
            format: Output format ("text", "json", "markdown")
            
        Returns:
            Formatted string
        """
        if format == "text":
            return self._format_text(recommendations)
        elif format == "markdown":
            return self._format_markdown(recommendations)
        else:
            return str(recommendations)
    
    def _format_text(self, recommendations: List[IndexRecommendation]) -> str:
        """Format as plain text."""
        if not recommendations:
            return "No index recommendations."
        
        lines = [f"Index Recommendations for {self.collection_name}:", "=" * 60]
        
        for i, rec in enumerate(recommendations, 1):
            lines.append(f"\n{i}. {rec}")
            lines.append("-" * 60)
        
        return "\n".join(lines)
    
    def _format_markdown(self, recommendations: List[IndexRecommendation]) -> str:
        """Format as Markdown."""
        if not recommendations:
            return "No index recommendations."
        
        lines = [
            f"## Index Recommendations for {self.collection_name}",
            ""
        ]
        
        for i, rec in enumerate(recommendations, 1):
            lines.append(f"### {i}. {rec.index_type.title()} Index - {rec.priority.value.upper()}")
            lines.append(f"**Fields:** `{', '.join(rec.fields)}`")
            lines.append(f"**Reason:** {rec.reason}")
            lines.append(f"**Command:**")
            lines.append(f"```javascript")
            lines.append(rec.command)
            lines.append(f"```")
            lines.append("")
        
        return "\n".join(lines)
