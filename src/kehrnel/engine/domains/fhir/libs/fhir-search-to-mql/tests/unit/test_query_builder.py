"""
Comprehensive tests for Phase 6 Query Builder.

Tests all builder components:
- MQLBuilder
- LogicCombiner
- QueryOptimizer
- QueryValidator
- IndexRecommender
"""

import pytest
from fhir_search_to_mql.builder import (
    MQLBuilder,
    QueryMetadata,
    LogicCombiner,
    QueryOptimizer,
    QueryValidator,
    ValidationResult,
    IndexRecommender,
    IndexRecommendation,
    IndexPriority,
)
from fhir_search_to_mql.core.exceptions import ConversionError


# ==================== LOGIC COMBINER TESTS ====================

class TestLogicCombiner:
    """Test LogicCombiner functionality."""
    
    @pytest.fixture
    def combiner(self):
        """Create LogicCombiner instance."""
        return LogicCombiner()
    
    def test_combine_and_different_fields(self, combiner):
        """Test AND combination with different fields."""
        queries = [{"name": "John"}, {"age": 30}]
        result = combiner.combine_and(queries)
        
        # Should merge into single dict
        assert "name" in result
        assert "age" in result
        assert result["name"] == "John"
        assert result["age"] == 30
    
    def test_combine_and_same_field(self, combiner):
        """Test AND combination with same field."""
        queries = [{"name": "John"}, {"name": "Jane"}]
        result = combiner.combine_and(queries)
        
        # Should use $and operator
        assert "$and" in result
        assert len(result["$and"]) == 2
    
    def test_combine_and_empty(self, combiner):
        """Test AND combination with empty list."""
        result = combiner.combine_and([])
        assert result == {}
    
    def test_combine_and_single(self, combiner):
        """Test AND combination with single query."""
        queries = [{"name": "John"}]
        result = combiner.combine_and(queries)
        
        # Should return query directly
        assert result == {"name": "John"}
    
    def test_combine_or(self, combiner):
        """Test OR combination."""
        queries = [{"name": "John"}, {"name": "Jane"}]
        result = combiner.combine_or(queries)
        
        assert "$or" in result
        assert len(result["$or"]) == 2
    
    def test_combine_or_empty(self, combiner):
        """Test OR combination with empty list."""
        result = combiner.combine_or([])
        assert result == {}
    
    def test_combine_or_single(self, combiner):
        """Test OR combination with single query."""
        queries = [{"name": "John"}]
        result = combiner.combine_or(queries)
        
        # Should return query directly
        assert result == {"name": "John"}
    
    def test_combine_nor(self, combiner):
        """Test NOR combination."""
        queries = [{"name": "John"}, {"name": "Jane"}]
        result = combiner.combine_nor(queries)
        
        assert "$nor" in result
        assert len(result["$nor"]) == 2
    
    def test_merge_adjacent_and(self, combiner):
        """Test merging adjacent AND conditions."""
        queries = [
            {"$and": [{"a": 1}, {"b": 2}]},
            {"c": 3}
        ]
        result = combiner.merge_adjacent_and(queries)
        
        # Should flatten nested $and
        assert len(result) == 3
        assert {"a": 1} in result
        assert {"b": 2} in result
        assert {"c": 3} in result
    
    def test_merge_adjacent_or(self, combiner):
        """Test merging adjacent OR conditions."""
        queries = [
            {"$or": [{"a": 1}, {"b": 2}]},
            {"c": 3}
        ]
        result = combiner.merge_adjacent_or(queries)
        
        # Should flatten nested $or
        assert len(result) == 3


# ==================== QUERY OPTIMIZER TESTS ====================

class TestQueryOptimizer:
    """Test QueryOptimizer functionality."""
    
    @pytest.fixture
    def optimizer(self):
        """Create QueryOptimizer instance."""
        return QueryOptimizer()
    
    def test_optimize_empty(self, optimizer):
        """Test optimizing empty query."""
        result = optimizer.optimize({})
        assert result == {}
    
    def test_simplify_single_element_and(self, optimizer):
        """Test simplifying single-element $and."""
        query = {"$and": [{"field": "value"}]}
        result = optimizer.optimize(query)
        
        # Should unwrap to simple query
        assert result == {"field": "value"}
    
    def test_simplify_single_element_or(self, optimizer):
        """Test simplifying single-element $or."""
        query = {"$or": [{"field": "value"}]}
        result = optimizer.optimize(query)
        
        # Should unwrap to simple query
        assert result == {"field": "value"}
    
    def test_flatten_nested_and(self, optimizer):
        """Test flattening nested $and."""
        query = {"$and": [{"a": 1}, {"$and": [{"b": 2}, {"c": 3}]}]}
        result = optimizer.optimize(query)
        
        # Should flatten and merge into single dict (most optimized)
        assert result == {"a": 1, "b": 2, "c": 3}
    
    def test_merge_adjacent_conditions(self, optimizer):
        """Test merging adjacent field conditions."""
        query = {"$and": [{"a": 1}, {"b": 2}, {"c": 3}]}
        result = optimizer.optimize(query)
        
        # Should merge into single dict
        assert "a" in result
        assert "b" in result
        assert "c" in result
        assert "$and" not in result
    
    def test_remove_redundant_conditions(self, optimizer):
        """Test removing redundant conditions."""
        query = {"$and": [{"a": 1}, {"a": 1}]}
        result = optimizer.optimize(query)
        
        # Should remove duplicate
        # After optimization, should be simplified
        assert result == {"a": 1}
    
    def test_estimate_complexity(self, optimizer):
        """Test complexity estimation."""
        query = {"$and": [{"a": 1}, {"b": 2}, {"c": 3}]}
        metrics = optimizer.estimate_complexity(query)
        
        assert "num_conditions" in metrics
        assert "max_depth" in metrics
        assert "performance" in metrics
        assert metrics["num_conditions"] >= 3
    
    def test_estimate_complexity_with_regex(self, optimizer):
        """Test complexity with regex."""
        query = {"name": {"$regex": "^John"}}
        metrics = optimizer.estimate_complexity(query)
        
        assert metrics["has_regex"] == True
        assert metrics["performance"] == "slow"


# ==================== QUERY VALIDATOR TESTS ====================

class TestQueryValidator:
    """Test QueryValidator functionality."""
    
    @pytest.fixture
    def config(self):
        """Sample resource configuration."""
        return {
            'name': {
                'type': 'string',
                'fields': [{'field': 'name', 'indexed': True}]
            },
            'birthDate': {
                'type': 'date',
                'fields': [{'field': 'birthDate', 'indexed': False}]
            }
        }
    
    @pytest.fixture
    def validator(self, config):
        """Create QueryValidator instance."""
        return QueryValidator(config)
    
    def test_parameter_exists(self, validator):
        """Test parameter existence check."""
        result = validator.validate_parameter("name", "John")
        
        assert result.is_valid == True
        assert len(result.errors) == 0
    
    def test_parameter_not_exists(self, validator):
        """Test non-existent parameter."""
        result = validator.validate_parameter("unknown", "value", resource_type="Patient")
        
        assert result.is_valid == False
        assert len(result.errors) > 0
        assert "not defined" in result.errors[0]
    
    def test_invalid_modifier(self, validator):
        """Test invalid modifier."""
        result = validator.validate_parameter("birthDate", "2024-01-01", modifier="exact")
        
        assert result.is_valid == False
        assert "not allowed" in result.errors[0]
    
    def test_invalid_prefix(self, validator):
        """Test invalid prefix."""
        result = validator.validate_parameter("name", "John", prefix="gt")
        
        assert result.is_valid == False
        assert "not allowed" in result.errors[0]
    
    def test_invalid_date_format(self, validator):
        """Test invalid date format."""
        result = validator.validate_parameter("birthDate", "invalid-date")
        
        assert result.is_valid == False
        assert "Invalid date format" in result.errors[0]
    
    def test_valid_date_formats(self, validator):
        """Test valid date formats."""
        valid_dates = ["2024", "2024-01", "2024-01-15", "2024-01-15T10:30:00"]
        
        for date in valid_dates:
            result = validator.validate_parameter("birthDate", date)
            assert result.is_valid == True
    
    def test_missing_index_warning(self, validator):
        """Test warning for missing index."""
        result = validator.validate_parameter("birthDate", "2024-01-01")
        
        # Should have warning about missing index
        assert len(result.warnings) > 0
        assert "No index found" in result.warnings[0]
    
    def test_validate_query_complexity(self, validator):
        """Test query complexity validation."""
        # Complex query with many conditions
        query = {
            "$and": [
                {"field1": "value1"},
                {"field2": "value2"},
                {"field3": "value3"},
                {"field4": "value4"},
                {"field5": "value5"},
                {"field6": "value6"},
                {"field7": "value7"},
                {"field8": "value8"},
                {"field9": "value9"},
                {"field10": "value10"},
                {"field11": "value11"}
            ]
        }
        
        result = validator.validate_query(query)
        
        # Should have warning about complexity
        assert len(result.warnings) > 0


# ==================== INDEX RECOMMENDER TESTS ====================

class TestIndexRecommender:
    """Test IndexRecommender functionality."""
    
    @pytest.fixture
    def recommender(self):
        """Create IndexRecommender instance."""
        return IndexRecommender(resource_type="Patient")
    
    def test_recommend_single_field(self, recommender):
        """Test single-field index recommendation."""
        query = {"name": "John"}
        recommendations = recommender.analyze(query)
        
        assert len(recommendations) > 0
        assert any("name" in rec.fields for rec in recommendations)
    
    def test_recommend_compound_index(self, recommender):
        """Test compound index recommendation."""
        query = {"$and": [{"name": "John"}, {"birthDate": "2024-01-01"}]}
        recommendations = recommender.analyze(query)
        
        # Should recommend compound index
        compound_recs = [r for r in recommendations if r.index_type == "compound"]
        assert len(compound_recs) > 0
    
    def test_recommend_text_index(self, recommender):
        """Test text index recommendation."""
        query = {"$text": {"$search": "diabetes"}}
        recommendations = recommender.analyze(query)
        
        # Should recommend text index
        text_recs = [r for r in recommendations if r.index_type == "text"]
        assert len(text_recs) > 0
        assert text_recs[0].priority == IndexPriority.CRITICAL
    
    def test_recommend_regex_index(self, recommender):
        """Test index recommendation for regex."""
        query = {"name": {"$regex": "^John"}}
        recommendations = recommender.analyze(query)
        
        # Should recommend text index as alternative
        assert len(recommendations) > 0
        assert any(rec.priority == IndexPriority.CRITICAL for rec in recommendations)
    
    def test_format_recommendations_text(self, recommender):
        """Test formatting recommendations as text."""
        query = {"name": "John"}
        recommendations = recommender.analyze(query)
        
        formatted = recommender.format_recommendations(recommendations, format="text")
        
        assert isinstance(formatted, str)
        assert "Index Recommendations" in formatted
    
    def test_format_recommendations_markdown(self, recommender):
        """Test formatting recommendations as markdown."""
        query = {"name": "John"}
        recommendations = recommender.analyze(query)
        
        formatted = recommender.format_recommendations(recommendations, format="markdown")
        
        assert isinstance(formatted, str)
        assert "##" in formatted


# ==================== MQL BUILDER TESTS ====================

class TestMQLBuilder:
    """Test MQLBuilder functionality."""
    
    @pytest.fixture
    def config(self):
        """Sample resource configuration."""
        return {
            'name': {
                'type': 'string',
                'fields': [{'field': 'name', 'indexed': True}]
            },
            'birthDate': {
                'type': 'date',
                'fields': [{'field': 'birthDate'}]
            }
        }
    
    @pytest.fixture
    def builder(self, config):
        """Create MQLBuilder instance."""
        return MQLBuilder(resource_type="Patient", config=config)
    
    def test_build_empty(self, builder):
        """Test building with empty queries."""
        result = builder.build([])
        assert result == {}
    
    def test_build_single(self, builder):
        """Test building with single query."""
        queries = [{"name": "John"}]
        result = builder.build(queries)
        
        assert result == {"name": "John"}
    
    def test_build_multiple_and(self, builder):
        """Test building with multiple queries (AND)."""
        queries = [{"name": "John"}, {"birthDate": "2024-01-01"}]
        result = builder.build(queries, logic="AND")
        
        # Should merge or use $and
        assert "name" in result or "$and" in result
    
    def test_build_multiple_or(self, builder):
        """Test building with multiple queries (OR)."""
        queries = [{"name": "John"}, {"name": "Jane"}]
        result = builder.build(queries, logic="OR")
        
        assert "$or" in result
        assert len(result["$or"]) == 2
    
    def test_build_with_optimization(self, builder):
        """Test building with optimization."""
        queries = [{"$and": [{"name": "John"}]}]
        result = builder.build(queries)
        
        # Should be optimized (unwrapped)
        assert result == {"name": "John"}
    
    def test_build_with_metadata(self, builder):
        """Test building with metadata."""
        queries = [{"name": "John"}, {"birthDate": "2024-01-01"}]
        result, metadata = builder.build_with_metadata(queries, parameter_names=["name", "birthDate"])
        
        assert isinstance(metadata, QueryMetadata)
        assert len(metadata.parsed_parameters) == 2
        assert "name" in metadata.parsed_parameters
        assert metadata.num_conditions > 0
    
    def test_explain(self, builder):
        """Test query explanation (dry-run)."""
        queries = [{"name": "John"}, {"birthDate": "2024-01-01"}]
        explanation = builder.explain(queries, parameter_names=["name", "birthDate"])
        
        assert "final_query" in explanation
        assert "parsed_parameters" in explanation
        assert "complexity" in explanation
        assert "index_recommendations" in explanation
        assert explanation["resource_type"] == "Patient"
    
    def test_add_compartment_filter(self, builder):
        """Test adding compartment filter."""
        query = {"name": "John"}
        compartment = {"_search.patientId": "123"}
        
        result = builder.add_compartment_filter(query, compartment)
        
        # Should combine with AND
        assert "_search.patientId" in str(result)
        assert "name" in str(result)
    
    def test_optimize(self, builder):
        """Test optimize method."""
        query = {"$and": [{"name": "John"}]}
        result = builder.optimize(query)
        
        # Should be simplified
        assert result == {"name": "John"}
    
    def test_get_index_recommendations(self, builder):
        """Test getting index recommendations."""
        query = {"name": {"$regex": "^John"}}
        recommendations = builder.get_index_recommendations(query)
        
        assert isinstance(recommendations, str)
        assert "Index Recommendations" in recommendations


# ==================== INTEGRATION TESTS ====================

class TestBuilderIntegration:
    """Integration tests for builder components."""
    
    def test_full_workflow(self):
        """Test complete workflow with all components."""
        config = {
            'name': {
                'type': 'string',
                'fields': [{'field': 'name', 'indexed': True}]
            }
        }
        
        # Create builder
        builder = MQLBuilder(resource_type="Patient", config=config)
        
        # Build queries
        queries = [{"name": "John"}, {"age": 30}]
        
        # Build with metadata
        result, metadata = builder.build_with_metadata(
            queries,
            parameter_names=["name", "age"]
        )
        
        # Verify result
        assert result is not None
        assert isinstance(metadata, QueryMetadata)
        assert len(metadata.parsed_parameters) == 2
        
        # Get explanation
        explanation = builder.explain(queries, parameter_names=["name", "age"])
        assert "final_query" in explanation
        assert "index_recommendations" in explanation
    
    def test_optimization_and_validation(self):
        """Test optimization and validation together."""
        builder = MQLBuilder(
            resource_type="Patient",
            enable_optimization=True,
            enable_validation=True
        )
        
        # Complex query that can be optimized
        queries = [
            {"$and": [{"name": "John"}]},
            {"age": 30}
        ]
        
        result = builder.build(queries)
        
        # Should be optimized
        assert "name" in result
        assert "age" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
