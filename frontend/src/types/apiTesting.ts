// API Testing Types for Live API Testing functionality

export interface APIRequest {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  path: string
  pathParams?: Record<string, string>
  queryParams?: Record<string, string>
  headers?: Record<string, string>
  body?: any
  auth?: {
    type: 'bearer' | 'apikey' | 'basic'
    value: string
  }
}

export interface APIResponse {
  status: number
  statusText: string
  headers: Record<string, string>
  data: any
  duration: number // milliseconds
  size: number // bytes
  timestamp: string
  success: boolean
}

export interface RequestHistory {
  id: string
  request: APIRequest
  response: APIResponse
  timestamp: string
}

// Form state for request builder
export interface RequestFormState {
  pathParams: Record<string, string>
  queryParams: Record<string, string>
  headers: Record<string, string>
  body: string // JSON string
  auth: {
    type: 'bearer' | 'apikey' | 'basic' | 'none'
    value: string
  }
}

// Test execution state
export interface TestState {
  isLoading: boolean
  request: APIRequest | null
  response: APIResponse | null
  error: string | null
}

// Parameter input component props
export interface ParameterInputProps {
  parameter: {
    name: string
    type: string
    required: boolean
    description?: string
    example?: any
  }
  value: string
  onChange: (value: string) => void
}

// Response viewer component props
export interface ResponseViewerProps {
  response: APIResponse | null
  isLoading: boolean
}

// Request history item
export interface HistoryItemProps {
  item: RequestHistory
  onSelect: (item: RequestHistory) => void
  onDelete: (id: string) => void
}

// Authentication configuration
export interface AuthConfig {
  type: 'bearer' | 'apikey' | 'basic' | 'none'
  value: string
  label: string
}

// Available authentication types
export const AUTH_TYPES = [
  { value: 'none', label: 'No Authentication' },
  { value: 'bearer', label: 'Bearer Token' },
  { value: 'apikey', label: 'API Key' },
  { value: 'basic', label: 'Basic Auth' }
] as const

// HTTP status code helpers
export interface StatusCodeInfo {
  code: number
  text: string
  type: 'success' | 'error' | 'info' | 'warning'
  description: string
}

export const getStatusCodeInfo = (status: number): StatusCodeInfo => {
  if (status >= 200 && status < 300) {
    return {
      code: status,
      text: 'Success',
      type: 'success',
      description: 'The request was successful'
    }
  } else if (status >= 300 && status < 400) {
    return {
      code: status,
      text: 'Redirect',
      type: 'info',
      description: 'The request was redirected'
    }
  } else if (status >= 400 && status < 500) {
    return {
      code: status,
      text: 'Client Error',
      type: 'error',
      description: 'There was an error with the request'
    }
  } else if (status >= 500) {
    return {
      code: status,
      text: 'Server Error',
      type: 'error',
      description: 'There was a server error'
    }
  } else if (status === 0) {
    return {
      code: status,
      text: 'Network Error',
      type: 'error',
      description: 'Unable to connect to the server'
    }
  } else {
    return {
      code: status,
      text: 'Unknown',
      type: 'warning',
      description: 'Unknown status code'
    }
  }
}

// Format file size
export const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes'
  
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// Format duration
export const formatDuration = (ms: number): string => {
  if (ms < 1000) {
    return `${ms}ms`
  } else {
    return `${(ms / 1000).toFixed(2)}s`
  }
}