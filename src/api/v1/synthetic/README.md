# Synthetic Data Generation Module

This module provides functionality for generating synthetic EHR data based on composition templates. It's designed for testing, development, and demonstration purposes within the OpenEHR MongoDB API.

## Overview

The synthetic data generator creates realistic test data by:

1. **Creating EHRs** with synthetic patient subjects
2. **Generating compositions** based on provided templates with randomized data
3. **Linking compositions to EHRs** following OpenEHR specifications
4. **Storing both canonical and flattened** versions of compositions

## Features

- ✅ **Template-based generation**: Use any OpenEHR composition as a template
- ✅ **Built-in vaccination template**: Default HC3 Immunization List template included
- ✅ **Data randomization**: Realistic variations in dates, identifiers, and clinical data
- ✅ **Batch generation**: Generate multiple records in a single request (up to 100)
- ✅ **Error handling**: Graceful handling of partial failures
- ✅ **Performance tracking**: Generation time and statistics
- ✅ **OpenEHR compliance**: Follows OpenEHR reference model specifications

## API Endpoints

### POST `/v1/synthetic/generate`

Generate synthetic EHR data with compositions.

**Parameters:**
- `count` (required): Number of records to generate (1-100)
- `base_composition` (optional): Custom composition template

**Response:** Detailed generation results with created EHR IDs, composition UIDs, and timing information.

### GET `/v1/synthetic/stats`

Get statistics about synthetic data generation.

**Response:** Success rates, timing, and totals.

## Data Randomization

The generator applies intelligent randomization to create realistic variations:

### Vaccination Template Randomizations

- **Vaccine Types**: Rotates between Meningococcal C, Hepatitis B, Tetanus-Diphtheria, Flu, and Pneumococcal vaccines
- **Patient Identifiers**: Generates unique synthetic patient IDs
- **Dates**: Randomizes vaccination dates (1-5 years ago) and document dates (1-12 months ago)
- **Healthcare Providers**: Randomizes provider names and professional identifiers
- **Medical Codes**: Generates varied medication codes and reference numbers
- **System Identifiers**: Creates unique feeder audit IDs and tracking numbers

### Custom Template Support

For custom compositions, the generator:
- Preserves the overall structure and archetype compliance
- Randomizes identifiers in `feeder_audit` sections
- Updates temporal data (dates and timestamps)
- Maintains referential integrity between related data elements

## Usage Examples

### Basic Usage (5 vaccination records)

```bash
curl -X POST "http://localhost:9000/v1/synthetic/generate" \
  -H "Content-Type: application/json" \
  -d '{"count": 5}'
```

### Custom Template Usage

```bash
curl -X POST "http://localhost:9000/v1/synthetic/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 10,
    "base_composition": {
      "_type": "COMPOSITION",
      "name": {"_type": "DV_TEXT", "value": "Custom Template"},
      "archetype_details": {
        "archetype_id": {"value": "openEHR-EHR-COMPOSITION.custom.v1"},
        "template_id": {"value": "Custom Template v1.0"}
      }
    }
  }'
```

## Architecture

### Components

1. **SyntheticDataGenerator**: Core class for composition randomization
2. **Service Layer** (`service.py`): Business logic for EHR and composition creation
3. **API Layer** (`routes.py`): REST endpoints and request/response handling
4. **Models** (`models.py`): Pydantic models for request/response validation
5. **Documentation** (`api_responses.py`): OpenAPI response schemas

### Integration Points

- **EHR Service**: Creates EHRs with synthetic subjects
- **Composition Service**: Handles composition creation and validation
- **Flattener**: Generates optimized search documents
- **MongoDB**: Stores canonical and flattened composition data

### Data Flow

```
Request → Validation → Template Loading → EHR Creation → 
Composition Generation → Randomization → Composition Creation → 
Flattening → Database Storage → Response
```

## Performance Considerations

- **Batch Size Limit**: Maximum 100 records per request to prevent timeouts
- **Async Processing**: All database operations are asynchronous
- **Error Isolation**: Individual record failures don't stop batch processing
- **Memory Efficient**: Processes records sequentially to limit memory usage

## Error Handling

The generator handles various failure scenarios:

- **Partial Failures**: Individual record errors are captured but don't stop the batch
- **Validation Errors**: Invalid templates are rejected before processing
- **Database Errors**: Connection issues and constraint violations are handled gracefully
- **Timeout Protection**: Reasonable limits prevent resource exhaustion

## Development and Testing

### Running Tests

```bash
# Import test
python3 -c "from src.api.v1.synthetic import routes; print('Import successful')"

# Integration test (requires running server)
curl -X POST "http://localhost:9000/v1/synthetic/generate" \
  -H "Content-Type: application/json" \
  -d '{"count": 1}'
```

### Customization

To add new randomization patterns:

1. Extend `SyntheticDataGenerator` with new `_randomize_*` methods
2. Call new methods in `generate_synthetic_composition()`
3. Add configuration options to support different domains

### Extending Templates

To support new composition types:

1. Add template detection logic in `SyntheticDataGenerator`
2. Implement domain-specific randomization methods
3. Update the default template in `routes.py` if needed

## Security Considerations

- **No Real Patient Data**: All generated data is synthetic
- **Isolated Namespace**: Uses "synthetic.data.namespace" for subjects
- **Audit Trail**: All operations are logged with "SyntheticDataGenerator" committer
- **Access Control**: Inherits API authentication/authorization patterns

## Future Enhancements

Planned improvements include:

- **Template Library**: Pre-built templates for common clinical scenarios
- **Smart Relationships**: Generate related compositions (e.g., follow-up visits)
- **Bulk Export**: Export generated data for external testing
- **Statistical Control**: Configure randomization parameters
- **Performance Metrics**: Enhanced monitoring and optimization
- **Data Validation**: Post-generation quality checks

## See Also

- [API Examples](./curl_examples.md) - Complete usage examples
- [Composition Service](../composition/) - Core composition handling
- [EHR Service](../ehr/) - EHR management
- [Flattener](../../../transform/) - Document transformation