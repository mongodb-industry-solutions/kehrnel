import { 
  OpenAPISpec, 
  OpenAPIOperation, 
  OpenAPIParameter, 
  OpenAPISchema,
  APIResource, 
  APIOperation, 
  Parameter, 
  RequestBody, 
  Response, 
  Example 
} from '@/types/openapi'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:9000'

/**
 * Fetch OpenAPI specification from FastAPI backend
 */
export async function fetchOpenAPISpec(): Promise<OpenAPISpec> {
  const response = await fetch(`${API_BASE_URL}/openapi.json`, {
    headers: {
      'Accept': 'application/json',
    },
    // Add timeout
    signal: AbortSignal.timeout(10000),
  })

  if (!response.ok) {
    throw new Error(`Failed to fetch OpenAPI spec: ${response.status} ${response.statusText}`)
  }

  return response.json()
}

/**
 * Convert OpenAPI schema to a simple type string
 */
function getSchemaType(schema: OpenAPISchema): string {
  if (schema.$ref) {
    // Extract the schema name from $ref
    return schema.$ref.split('/').pop() || 'object'
  }
  
  if (schema.type === 'array' && schema.items) {
    return `${getSchemaType(schema.items)}[]`
  }
  
  if (schema.enum) {
    return 'enum'
  }
  
  return schema.type || 'any'
}

/**
 * Convert OpenAPI parameter to our Parameter interface
 */
function convertParameter(param: OpenAPIParameter): Parameter {
  return {
    name: param.name,
    in: param.in as 'path' | 'query' | 'header',
    required: param.required || false,
    type: getSchemaType(param.schema),
    description: param.description || param.schema.description || '',
    example: param.example || param.schema.example
  }
}

/**
 * Convert OpenAPI request body to our RequestBody interface
 */
function convertRequestBody(requestBody: OpenAPIOperation['requestBody']): RequestBody | undefined {
  if (!requestBody) return undefined

  const content: { [contentType: string]: { schema: any; example: any } } = {}
  
  for (const [mediaType, mediaTypeObject] of Object.entries(requestBody.content)) {
    content[mediaType] = {
      schema: mediaTypeObject.schema,
      example: mediaTypeObject.example || mediaTypeObject.schema.example || {}
    }
  }

  return {
    description: requestBody.description || 'Request body',
    content
  }
}

/**
 * Convert OpenAPI responses to our Response interface
 */
function convertResponses(responses: OpenAPIOperation['responses']): Response[] {
  const convertedResponses: Response[] = []

  for (const [statusCode, response] of Object.entries(responses)) {
    let example = null
    
    if (response.content) {
      // Get example from the first available content type (usually application/json)
      const firstContentType = Object.keys(response.content)[0]
      if (firstContentType && response.content[firstContentType]) {
        const contentTypeObj = response.content[firstContentType]
        
        // Try to get example from multiple possible locations
        if (contentTypeObj.example && typeof contentTypeObj.example === 'object' && Object.keys(contentTypeObj.example).length > 0) {
          example = contentTypeObj.example
        } else if (contentTypeObj.schema?.example && typeof contentTypeObj.schema.example === 'object' && Object.keys(contentTypeObj.schema.example).length > 0) {
          example = contentTypeObj.schema.example
        }
        // If no examples found, leave as null - examples should be provided by the backend OpenAPI spec
      }
    }

    convertedResponses.push({
      status: statusCode,
      description: response.description,
      example
    })
  }

  return convertedResponses
}

/**
 * Generate examples from OpenAPI operation
 */
function generateExamples(operation: OpenAPIOperation, path: string, method: string): Example[] {
  const examples: Example[] = []
  
  // Create a basic example from the operation
  const title = operation.summary || `${method.toUpperCase()} ${path}`
  const description = operation.description || `Example for ${title}`
  
  // Try to get example from responses
  let responseExample = {}
  const successResponse = operation.responses['200'] || operation.responses['201']
  if (successResponse?.content) {
    const firstContentType = Object.keys(successResponse.content)[0]
    if (firstContentType) {
      responseExample = successResponse.content[firstContentType].example || 
                       successResponse.content[firstContentType].schema?.example || 
                       {}
    }
  }

  examples.push({
    title,
    description,
    response: responseExample
  })

  return examples
}

/**
 * Convert OpenAPI operation to our APIOperation interface
 */
function convertOperation(
  path: string, 
  method: string, 
  operation: OpenAPIOperation
): APIOperation {
  return {
    method: method.toUpperCase() as 'GET' | 'POST' | 'PUT' | 'DELETE',
    path,
    summary: operation.summary || `${method.toUpperCase()} ${path}`,
    description: operation.description || '',
    parameters: operation.parameters?.map(convertParameter) || [],
    requestBody: convertRequestBody(operation.requestBody),
    responses: convertResponses(operation.responses),
    examples: generateExamples(operation, path, method)
  }
}

/**
 * Group operations by tags (resources)
 */
function groupOperationsByTag(spec: OpenAPISpec): Map<string, APIOperation[]> {
  const groupedOperations = new Map<string, APIOperation[]>()

  for (const [path, pathItem] of Object.entries(spec.paths)) {
    for (const [method, operation] of Object.entries(pathItem)) {
      const apiOperation = convertOperation(path, method, operation)
      
      // Get the first tag or use 'default'
      const tag = operation.tags?.[0] || 'default'
      
      if (!groupedOperations.has(tag)) {
        groupedOperations.set(tag, [])
      }
      
      groupedOperations.get(tag)!.push(apiOperation)
    }
  }

  return groupedOperations
}

/**
 * Get tag description from OpenAPI spec
 */
function getTagDescription(spec: OpenAPISpec, tagName: string): string {
  const tag = spec.tags?.find(t => t.name === tagName)
  return tag?.description || `${tagName} operations`
}

/**
 * Convert OpenAPI spec to our APIResource array
 */
export function convertOpenAPIToResources(spec: OpenAPISpec): APIResource[] {
  const groupedOperations = groupOperationsByTag(spec)
  const resources: APIResource[] = []

  for (const [tagName, operations] of groupedOperations.entries()) {
    // Skip internal or utility tags if needed
    if (tagName === 'default' && operations.length === 0) continue
    
    resources.push({
      name: tagName,
      description: getTagDescription(spec, tagName),
      operations: operations.sort((a, b) => {
        // Sort operations by path then method
        if (a.path !== b.path) {
          return a.path.localeCompare(b.path)
        }
        return a.method.localeCompare(b.method)
      })
    })
  }

  // Sort resources by name
  return resources.sort((a, b) => a.name.localeCompare(b.name))
}

/**
 * Main function to fetch and convert OpenAPI spec to API resources
 */
export async function getAPIResources(): Promise<APIResource[]> {
  try {
    const spec = await fetchOpenAPISpec()
    return convertOpenAPIToResources(spec)
  } catch (error) {
    console.error('Failed to fetch API resources:', error)
    throw error
  }
}

/**
 * Get available resource names for navigation
 */
export async function getResourceNames(): Promise<string[]> {
  try {
    const resources = await getAPIResources()
    return resources.map(resource => resource.name)
  } catch (error) {
    console.error('Failed to fetch resource names:', error)
    return []
  }
}