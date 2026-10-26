"""
Comprehensive tests for Phase 5 Advanced Converters.

Tests all advanced converters:
- ReferenceConverter
- URIConverter
- CompositeConverter
- SpecialConverter
- ChainingHandler
- MultiStepQuery
"""

import pytest
from fhir_search_to_mql.converters import (
    ReferenceConverter,
    URIConverter,
    CompositeConverter,
    SpecialConverter,
    ChainingHandler,
    MultiStepQuery,
    QueryStep,
)
from fhir_search_to_mql.core.exceptions import ConversionError


# ==================== REFERENCE CONVERTER TESTS ====================

class TestReferenceConverter:
    """Test ReferenceConverter with various formats and modifiers."""
    
    @pytest.fixture
    def reference_config(self):
        """Sample configuration for reference parameter."""
        return {
            'type': 'reference',
            'fields': [
                {'field': '_search.patientId', 'referenceType': 'Patient'},
                {'field': '_search.subjectId'}  # Generic
            ],
            'referenceTarget': 'Patient'
        }
    
    @pytest.fixture
    def converter(self, reference_config):
        """Create ReferenceConverter instance."""
        return ReferenceConverter(reference_config)
    
    def test_simple_id(self, converter):
        """Test simple ID reference."""
        query = converter.convert("123")
        
        assert '_search.patientId' in str(query) or '_search.subjectId' in str(query)
    
    def test_type_and_id(self, converter):
        """Test Type/ID format."""
        query = converter.convert("Patient/123")
        
        # Should prefer type-specific field
        assert '_search.patientId' in str(query)
    
    def test_full_url(self, converter):
        """Test full URL format."""
        query = converter.convert("https://example.org/fhir/Patient/123")
        
        assert query is not None
    
    def test_type_modifier(self, converter):
        """Test :Patient type modifier.

        The query parser strips the leading colon, so converters receive the
        bare resource type name ("Patient") rather than ":Patient".
        """
        query = converter.convert("123", modifier="Patient")

        assert '_search.patientId' in str(query)
    
    def test_identifier_modifier(self, converter):
        """Test :identifier modifier (multi-step)."""
        result = converter.convert("system|value", modifier="identifier")
        
        assert isinstance(result, MultiStepQuery)
        assert result.is_multi_step
        assert len(result.steps) > 0
    
    def test_missing_true(self, converter):
        """Test :missing=true."""
        query = converter.convert("true", modifier="missing")
        
        assert '$or' in query
    
    def test_missing_false(self, converter):
        """Test :missing=false."""
        query = converter.convert("false", modifier="missing")
        
        assert '$exists' in str(query)


# ==================== URI CONVERTER TESTS ====================

class TestURIConverter:
    """Test URIConverter with hierarchical searches."""
    
    @pytest.fixture
    def uri_config(self):
        """Sample configuration for URI parameter."""
        return {
            'type': 'uri',
            'fields': [{'field': 'url', 'query_type': 'range'}]
        }
    
    @pytest.fixture
    def converter(self, uri_config):
        """Create URIConverter instance."""
        return URIConverter(uri_config)
    
    def test_exact_match(self, converter):
        """Test exact URI match."""
        query = converter.convert("http://example.org/ValueSet/123")
        
        assert 'url' in query
        assert query['url'] == "http://example.org/ValueSet/123"
    
    def test_below_modifier_range(self, converter):
        """Test :below modifier with range query."""
        query = converter.convert("http://example.org/", modifier="below")
        
        assert 'url' in query
        assert '$gte' in query['url']
        assert query['url']['$gte'] == "http://example.org/"
        assert '$lt' in query['url']
        assert '\uffff' in query['url']['$lt']
    
    def test_below_modifier_regex(self):
        """Test :below modifier with regex fallback."""
        config = {
            'type': 'uri',
            'fields': [{'field': 'url', 'query_type': 'regex'}]
        }
        converter = URIConverter(config)
        query = converter.convert("http://example.org/", modifier="below")
        
        assert 'url' in query
        assert '$regex' in query['url']
    
    def test_above_modifier(self, converter):
        """Test :above modifier for hierarchical parents."""
        query = converter.convert("http://example.org/path/to/resource", modifier="above")
        
        assert '$or' in query
        # Should have multiple parent URLs
        parent_urls = [q['url'] for q in query['$or']]
        assert len(parent_urls) > 0
    
    def test_missing_modifier(self, converter):
        """Test :missing modifier."""
        query = converter.convert("true", modifier="missing")
        
        assert '$or' in query


# ==================== COMPOSITE CONVERTER TESTS ====================

class TestCompositeConverter:
    """Test CompositeConverter for multi-component searches."""
    
    @pytest.fixture
    def composite_config(self):
        """Sample configuration for composite parameter."""
        return {
            'type': 'composite',
            'components': [
                {
                    'name': 'code',
                    'type': 'token',
                    'fields': [{'field': 'code.coding', 'tokenType': 'systemCode'}]
                },
                {
                    'name': 'value',
                    'type': 'quantity',
                    'fields': [{'field': 'valueQuantity'}]
                }
            ]
        }
    
    @pytest.fixture
    def converter(self, composite_config):
        """Create CompositeConverter instance."""
        return CompositeConverter(composite_config)
    
    def test_two_components(self, converter):
        """Test composite with two components."""
        query = converter.convert("http://loinc.org|2093-3$le5")
        
        assert '$and' in query
        assert len(query['$and']) == 2
    
    def test_component_types(self, converter):
        """Test that components use correct converters."""
        query = converter.convert("system|code$5.4")
        
        # Should have token and quantity queries
        assert '$and' in query
    
    def test_wrong_component_count(self, converter):
        """Test error with wrong number of components."""
        with pytest.raises(ConversionError):
            converter.convert("value1")  # Only 1 component, expected 2


# ==================== SPECIAL CONVERTER TESTS ====================

class TestSpecialConverter:
    """Test SpecialConverter for FHIR special parameters."""
    
    def test_id_single(self):
        """Test _id with single value."""
        query = SpecialConverter.convert_id("123")
        
        assert query == {"_id": "123"}
    
    def test_id_multiple(self):
        """Test _id with multiple values."""
        query = SpecialConverter.convert_id("123,456,789")
        
        assert "_id" in query
        assert "$in" in query["_id"]
        assert "123" in query["_id"]["$in"]
        assert "456" in query["_id"]["$in"]
        assert "789" in query["_id"]["$in"]
    
    def test_last_updated(self):
        """Test _lastUpdated parameter."""
        query = SpecialConverter.convert_last_updated("2024-01-01", prefix="ge")
        
        assert "_search._dates._lastUpdated" in str(query)
        assert "$gt" in str(query)
    
    def test_tag_system_code(self):
        """Test _tag with system|code."""
        query = SpecialConverter.convert_tag("http://terminology.org|tag1")
        
        assert "meta.tag" in query
        assert "$elemMatch" in query["meta.tag"]
    
    def test_tag_code_only(self):
        """Test _tag with code only."""
        query = SpecialConverter.convert_tag("tag1")
        
        assert "meta.tag" in query
        assert "$elemMatch" in query["meta.tag"]
    
    def test_tag_missing(self):
        """Test _tag with :missing."""
        query = SpecialConverter.convert_tag("true", modifier="missing")
        
        assert "$or" in query
    
    def test_tag_not_modifier(self):
        """Test _tag with :not modifier."""
        query = SpecialConverter.convert_tag("tag1", modifier="not")
        
        assert "$nor" in query
    
    def test_profile(self):
        """Test _profile parameter."""
        query = SpecialConverter.convert_profile("http://hl7.org/fhir/StructureDefinition/Patient")
        
        assert "meta.profile" in query
    
    def test_security(self):
        """Test _security parameter."""
        query = SpecialConverter.convert_security("system|code")
        
        assert "meta.security" in query
        assert "$elemMatch" in query["meta.security"]
    
    def test_has_reverse_chaining(self):
        """Test _has for reverse chaining."""
        result = SpecialConverter.convert_has("Observation:subject:code=8480-6", "Patient")
        
        assert isinstance(result, MultiStepQuery)
        assert result.is_multi_step
        assert len(result.steps) > 0
    
    def test_text_search(self):
        """Test _text for narrative search."""
        query = SpecialConverter.convert_text("diabetes")
        
        assert "$text" in query
        assert query["$text"]["$search"] == "diabetes"
    
    def test_content_search(self):
        """Test _content for full resource search."""
        query = SpecialConverter.convert_content("blood pressure")
        
        assert "$text" in query
        assert query["$text"]["$search"] == "blood pressure"


# ==================== CHAINING HANDLER TESTS ====================

class TestChainingHandler:
    """Test ChainingHandler for reference chaining."""
    
    @pytest.fixture
    def handler(self):
        """Create ChainingHandler instance."""
        return ChainingHandler()
    
    def test_supports_chaining_true(self, handler):
        """Test detection of chaining syntax."""
        assert handler.supports_chaining("subject:Patient.name") == True
    
    def test_supports_chaining_false(self, handler):
        """Test detection of non-chaining syntax."""
        assert handler.supports_chaining("name:exact") == False
        assert handler.supports_chaining("name") == False
    
    def test_extract_base_parameter(self, handler):
        """Test extraction of base parameter."""
        assert handler.extract_base_parameter("subject:Patient.name") == "subject"
        assert handler.extract_base_parameter("name:exact") == "name"
        assert handler.extract_base_parameter("name") == "name"
    
    def test_parse_simple_chain(self, handler):
        """Test parsing simple chain."""
        result = handler.parse_chain("subject:Patient.name", "Smith", "Observation")
        
        assert isinstance(result, MultiStepQuery)
        assert len(result.steps) > 0
    
    def test_parse_chain_syntax(self, handler):
        """Test parsing chain syntax."""
        parts = handler._parse_chain_syntax("subject:Patient.name")
        
        assert len(parts) == 1
        assert parts[0]['reference_param'] == 'subject'
        assert parts[0]['resource_type'] == 'Patient'
        assert parts[0]['search_param'] == 'name'
    
    def test_parse_deep_chain_syntax(self, handler):
        """Test parsing deep chain."""
        parts = handler._parse_chain_syntax("subject:Patient.organization:Organization.name")
        
        assert len(parts) == 2
        assert parts[0]['resource_type'] == 'Patient'
        assert parts[1]['resource_type'] == 'Organization'


# ==================== MULTI-STEP QUERY TESTS ====================

class TestMultiStepQuery:
    """Test MultiStepQuery functionality."""
    
    def test_create_query(self):
        """Test creating multi-step query."""
        query = MultiStepQuery(description="Test query")
        
        assert query.is_multi_step == True
        assert query.description == "Test query"
        assert len(query.steps) == 0
    
    def test_add_step(self):
        """Test adding query steps."""
        query = MultiStepQuery()
        
        query.add_step(
            resource_type="Patient",
            query={"name": "Smith"},
            extract_field="id",
            description="Find patients"
        )
        
        assert len(query.steps) == 1
        assert query.steps[0].resource_type == "Patient"
        assert query.steps[0].query == {"name": "Smith"}
    
    def test_set_final_query_builder(self):
        """Test setting final query builder."""
        query = MultiStepQuery()
        
        query.set_final_query_builder(
            lambda ids: {"_id": {"$in": ids}}
        )
        
        assert query.final_query_builder is not None
    
    def test_execution_plan(self):
        """Test getting execution plan."""
        query = MultiStepQuery(description="Test")
        query.add_step("Patient", {"name": "Smith"}, "id")
        
        plan = query.get_execution_plan()
        
        assert plan['is_multi_step'] == True
        assert plan['num_steps'] == 1
        assert 'steps' in plan
    
    def test_query_step(self):
        """Test QueryStep creation."""
        step = QueryStep(
            resource_type="Patient",
            query={"name": "Smith"},
            extract_field="id",
            description="Test step"
        )
        
        assert step.resource_type == "Patient"
        assert step.query == {"name": "Smith"}
        assert step.extract_field == "id"
        assert step.description == "Test step"


# ==================== INTEGRATION TESTS ====================

class TestAdvancedConverterIntegration:
    """Integration tests across advanced converters."""
    
    def test_reference_to_multi_step(self):
        """Test reference converter returning multi-step query."""
        config = {
            'type': 'reference',
            'fields': [{'field': '_search.patientId'}],
            'referenceTarget': 'Patient'
        }
        converter = ReferenceConverter(config)
        
        result = converter.convert("system|value", modifier="identifier")
        
        assert isinstance(result, MultiStepQuery)
    
    def test_special_has_multi_step(self):
        """Test special _has returning multi-step query."""
        result = SpecialConverter.convert_has("Observation:subject:code=8480-6", "Patient")
        
        assert isinstance(result, MultiStepQuery)
        plan = result.get_execution_plan()
        assert plan['is_multi_step'] == True
    
    def test_chaining_multi_step(self):
        """Test chaining handler returning multi-step query."""
        handler = ChainingHandler()
        result = handler.parse_chain("subject:Patient.name", "Smith", "Observation")
        
        assert isinstance(result, MultiStepQuery)
        assert len(result.steps) > 0
    
    def test_composite_uses_converters(self):
        """Test composite converter uses other converters."""
        config = {
            'type': 'composite',
            'components': [
                {
                    'name': 'code',
                    'type': 'token',
                    'fields': [{'field': 'code'}]
                },
                {
                    'name': 'value',
                    'type': 'number',
                    'fields': [{'field': 'value'}]
                }
            ]
        }
        converter = CompositeConverter(config)
        
        query = converter.convert("code$100")
        
        # Should have AND with both components
        assert '$and' in query
        assert len(query['$and']) == 2


# ==================== ERROR HANDLING TESTS ====================

class TestAdvancedConverterErrors:
    """Test error handling in advanced converters."""
    
    def test_invalid_has_format(self):
        """Test error on invalid _has format."""
        with pytest.raises(ConversionError):
            SpecialConverter.convert_has("invalid", "Patient")
    
    def test_composite_wrong_components(self):
        """Test error on wrong component count."""
        config = {
            'type': 'composite',
            'components': [
                {'name': 'c1', 'type': 'token', 'fields': [{'field': 'f1'}]},
                {'name': 'c2', 'type': 'token', 'fields': [{'field': 'f2'}]}
            ]
        }
        converter = CompositeConverter(config)
        
        with pytest.raises(ConversionError):
            converter.convert("value1")  # Missing second component
    
    def test_composite_no_components(self):
        """Test error when no components defined."""
        config = {'type': 'composite', 'components': []}
        converter = CompositeConverter(config)
        
        with pytest.raises(ConversionError):
            converter.convert("value")
    
    def test_reference_invalid_modifier(self):
        """Test error on invalid modifier."""
        config = {'type': 'reference', 'fields': [{'field': 'field'}]}
        converter = ReferenceConverter(config)
        
        with pytest.raises(ConversionError):
            converter.convert("value", modifier="invalid_modifier")
    
    def test_uri_invalid_modifier(self):
        """Test error on invalid modifier."""
        config = {'type': 'uri', 'fields': [{'field': 'url'}]}
        converter = URIConverter(config)
        
        with pytest.raises(ConversionError):
            converter.convert("http://example.org", modifier="invalid_modifier")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
