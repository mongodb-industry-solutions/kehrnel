// OpenAPI Types (matching FastAPI's OpenAPI 3.0 schema)
export interface OpenAPISpec {
  openapi: string
  info: {
    title: string
    version: string
    description?: string
  }
  servers?: Array<{
    url: string
    description?: string
  }>
  paths: {
    [path: string]: {
      [method: string]: OpenAPIOperation
    }
  }
  components?: {
    schemas?: {
      [schemaName: string]: OpenAPISchema
    }
    responses?: {
      [responseName: string]: OpenAPIResponse
    }
  }
  tags?: Array<{
    name: string
    description?: string
  }>
}

export interface OpenAPIOperation {
  operationId?: string
  tags?: string[]
  summary?: string
  description?: string
  parameters?: OpenAPIParameter[]
  requestBody?: {
    description?: string
    required?: boolean
    content: {
      [mediaType: string]: {
        schema: OpenAPISchema
        example?: any
      }
    }
  }
  responses: {
    [statusCode: string]: OpenAPIResponse
  }
}

export interface OpenAPIParameter {
  name: string
  in: 'query' | 'header' | 'path' | 'cookie'
  description?: string
  required?: boolean
  schema: OpenAPISchema
  example?: any
}

export interface OpenAPIResponse {
  description: string
  content?: {
    [mediaType: string]: {
      schema: OpenAPISchema
      example?: any
    }
  }
}

export interface OpenAPISchema {
  type?: string
  format?: string
  items?: OpenAPISchema
  properties?: {
    [propertyName: string]: OpenAPISchema
  }
  required?: string[]
  example?: any
  description?: string
  enum?: any[]
  $ref?: string
}

// Application Types (existing interfaces from APIExplorer)
export interface APIResource {
  name: string
  description: string
  operations: APIOperation[]
}

export interface APIOperation {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE'
  path: string
  summary: string
  description: string
  parameters?: Parameter[]
  requestBody?: RequestBody
  responses: Response[]
  examples: Example[]
}

export interface Parameter {
  name: string
  in: 'path' | 'query' | 'header'
  required: boolean
  type: string
  description: string
  example?: string
}

export interface RequestBody {
  description: string
  content: {
    [contentType: string]: {
      schema: any
      example: any
    }
  }
}

export interface Response {
  status: string
  description: string
  example: any
}

export interface Example {
  title: string
  description: string
  request?: string
  response: any
}