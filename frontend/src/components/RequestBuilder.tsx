'use client'

import { useState, useEffect } from 'react'
import { PaperAirplaneIcon, TrashIcon, PlusIcon } from '@heroicons/react/24/outline'
import { APIOperation, Parameter } from '@/types/openapi'
import { APIRequest, RequestFormState, AUTH_TYPES } from '../types/apiTesting'

interface RequestBuilderProps {
  operation: APIOperation
  onExecute: (request: APIRequest) => Promise<void>
  isLoading: boolean
}

const RequestBuilder: React.FC<RequestBuilderProps> = ({ 
  operation, 
  onExecute, 
  isLoading 
}) => {
  const [formState, setFormState] = useState<RequestFormState>({
    pathParams: {},
    queryParams: {},
    headers: {},
    body: '',
    auth: { type: 'none', value: '' }
  })

  const [activeTab, setActiveTab] = useState<'params' | 'headers' | 'body' | 'auth'>('params')
  const [bodyError, setBodyError] = useState<string>('')
  const [selectedContentType, setSelectedContentType] = useState<string>('application/json')

  // Initialize form state when operation changes
  useEffect(() => {
    initializeFormState()
  }, [operation])

  // Get available content types for the operation
  const getAvailableContentTypes = (): string[] => {
    if (!operation.requestBody?.content) {
      return ['application/json'] // Default fallback
    }
    return Object.keys(operation.requestBody.content)
  }

  // Check if content type expects JSON
  const isJSONContentType = (contentType: string): boolean => {
    return contentType.includes('application/json') || contentType.includes('application/vnd.api+json')
  }

  // Check if content type expects plain text
  const isTextContentType = (contentType: string): boolean => {
    return contentType.includes('text/') || 
           contentType.includes('application/x-aql') ||
           contentType.includes('plain')
  }

  const initializeFormState = () => {
    const pathParams: Record<string, string> = {}
    const queryParams: Record<string, string> = {}
    
    // Initialize parameters with examples or empty values
    operation.parameters?.forEach(param => {
      const defaultValue = param.example?.toString() || ''
      if (param.in === 'path') {
        pathParams[param.name] = defaultValue
      } else if (param.in === 'query') {
        queryParams[param.name] = defaultValue
      }
    })

    // Initialize request body with example if available
    let bodyContent = ''
    const availableContentTypes = getAvailableContentTypes()
    const defaultContentType = availableContentTypes[0] || 'application/json'
    
    // Set the default content type
    setSelectedContentType(defaultContentType)
    
    if (operation.requestBody?.content?.[defaultContentType]?.example) {
      const example = operation.requestBody.content[defaultContentType].example
      
      // Only set body content if example is meaningful (not empty object, null, etc.)
      if (example && example !== null && !(typeof example === 'object' && Object.keys(example).length === 0)) {
        // Format based on content type
        if (isJSONContentType(defaultContentType)) {
          bodyContent = typeof example === 'string' ? example : JSON.stringify(example, null, 2)
        } else {
          bodyContent = typeof example === 'string' ? example : String(example)
        }
      }
    }

    setFormState({
      pathParams,
      queryParams,
      headers: {},
      body: bodyContent,
      auth: { type: 'none', value: '' }
    })
    setBodyError('')
  }

  const updatePathParam = (name: string, value: string) => {
    setFormState(prev => ({
      ...prev,
      pathParams: { ...prev.pathParams, [name]: value }
    }))
  }

  const updateQueryParam = (name: string, value: string) => {
    setFormState(prev => ({
      ...prev,
      queryParams: { ...prev.queryParams, [name]: value }
    }))
  }

  const updateHeader = (name: string, value: string) => {
    setFormState(prev => ({
      ...prev,
      headers: { ...prev.headers, [name]: value }
    }))
  }

  const addHeader = () => {
    const name = prompt('Header name:')
    if (name && name.trim()) {
      updateHeader(name.trim(), '')
    }
  }

  const removeHeader = (name: string) => {
    setFormState(prev => {
      const newHeaders = { ...prev.headers }
      delete newHeaders[name]
      return { ...prev, headers: newHeaders }
    })
  }

  const updateBody = (value: string) => {
    setFormState(prev => ({ ...prev, body: value }))
    
    // Validate based on selected content type
    if (value.trim()) {
      if (isJSONContentType(selectedContentType)) {
        try {
          JSON.parse(value)
          setBodyError('')
        } catch (error) {
          setBodyError('Invalid JSON format')
        }
      } else {
        // For non-JSON content types, no strict validation needed
        setBodyError('')
      }
    } else {
      setBodyError('')
    }
  }

  const updateContentType = (contentType: string) => {
    setSelectedContentType(contentType)
    
    // Clear any validation errors when switching content types
    setBodyError('')
    
    // If switching to JSON and body is not empty, validate it
    if (isJSONContentType(contentType) && formState.body.trim()) {
      try {
        JSON.parse(formState.body)
        setBodyError('')
      } catch (error) {
        setBodyError('Invalid JSON format')
      }
    }
  }

  const updateAuth = (type: 'bearer' | 'apikey' | 'basic' | 'none', value?: string) => {
    setFormState(prev => ({
      ...prev,
      auth: { 
        type, 
        value: value !== undefined ? value : prev.auth.value 
      }
    }))
  }

  const handleExecute = async () => {
    // Validate required path parameters
    const requiredPathParams = operation.parameters?.filter(p => p.in === 'path' && p.required) || []
    const missingPathParams = requiredPathParams.filter(p => !formState.pathParams[p.name]?.trim())
    
    if (missingPathParams.length > 0) {
      alert(`Missing required path parameters: ${missingPathParams.map(p => p.name).join(', ')}`)
      return
    }

    // Process body based on content type
    let processedBody = null
    if (formState.body.trim()) {
      if (isJSONContentType(selectedContentType)) {
        try {
          processedBody = JSON.parse(formState.body)
        } catch (error) {
          alert('Invalid JSON in request body')
          return
        }
      } else {
        // For non-JSON content types, send as plain text
        processedBody = formState.body
      }
    }

    // Build headers with proper content type
    const requestHeaders = { ...formState.headers }
    if (formState.body.trim()) {
      requestHeaders['Content-Type'] = selectedContentType
    }

    // Build the request
    const request: APIRequest = {
      method: operation.method,
      path: operation.path,
      pathParams: formState.pathParams,
      queryParams: formState.queryParams,
      headers: requestHeaders,
      body: processedBody,
      contentType: selectedContentType, // Add content type to request
      auth: formState.auth.type !== 'none' ? {
        type: formState.auth.type as 'bearer' | 'apikey' | 'basic',
        value: formState.auth.value
      } : undefined
    }

    await onExecute(request)
  }

  const renderParameters = () => {
    const pathParams = operation.parameters?.filter(p => p.in === 'path') || []
    const queryParams = operation.parameters?.filter(p => p.in === 'query') || []

    if (pathParams.length === 0 && queryParams.length === 0) {
      return (
        <div className="text-gray-500 text-sm p-4">
          No parameters required for this operation
        </div>
      )
    }

    return (
      <div className="space-y-4">
        {pathParams.length > 0 && (
          <div>
            <h4 className="font-medium text-gray-900 mb-2">Path Parameters</h4>
            <div className="space-y-2">
              {pathParams.map(param => (
                <ParameterInput
                  key={param.name}
                  parameter={param}
                  value={formState.pathParams[param.name] || ''}
                  onChange={(value) => updatePathParam(param.name, value)}
                />
              ))}
            </div>
          </div>
        )}

        {queryParams.length > 0 && (
          <div>
            <h4 className="font-medium text-gray-900 mb-2">Query Parameters</h4>
            <div className="space-y-2">
              {queryParams.map(param => (
                <ParameterInput
                  key={param.name}
                  parameter={param}
                  value={formState.queryParams[param.name] || ''}
                  onChange={(value) => updateQueryParam(param.name, value)}
                />
              ))}
            </div>
          </div>
        )}
      </div>
    )
  }

  const renderHeaders = () => {
    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h4 className="font-medium text-gray-900">Custom Headers</h4>
          <button
            onClick={addHeader}
            className="text-blue-600 hover:text-blue-700 text-sm flex items-center gap-1"
          >
            <PlusIcon className="h-4 w-4" />
            Add Header
          </button>
        </div>
        
        {Object.keys(formState.headers).length === 0 ? (
          <div className="text-gray-500 text-sm p-4">
            No custom headers. Click "Add Header" to add one.
          </div>
        ) : (
          <div className="space-y-2">
            {Object.entries(formState.headers).map(([name, value]) => (
              <div key={name} className="flex gap-2">
                <input
                  type="text"
                  value={name}
                  readOnly
                  className="w-1/3 px-3 py-2 border border-gray-300 rounded-md bg-gray-50 text-sm"
                />
                <input
                  type="text"
                  value={value}
                  onChange={(e) => updateHeader(name, e.target.value)}
                  placeholder="Header value"
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
                />
                <button
                  onClick={() => removeHeader(name)}
                  className="px-3 py-2 text-red-600 hover:text-red-700"
                >
                  <TrashIcon className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    )
  }

  const renderBody = () => {
    if (!operation.requestBody) {
      return (
        <div className="text-gray-500 text-sm p-4">
          This operation does not accept a request body
        </div>
      )
    }

    const availableContentTypes = getAvailableContentTypes()
    const placeholder = isJSONContentType(selectedContentType) 
      ? "Enter JSON request body..." 
      : isTextContentType(selectedContentType)
      ? "Enter plain text request body..."
      : "Enter request body..."

    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h4 className="font-medium text-gray-900">Request Body</h4>
          {availableContentTypes.length > 1 && (
            <div className="flex items-center gap-2">
              <label className="text-sm font-medium text-gray-700">Content-Type:</label>
              <select
                value={selectedContentType}
                onChange={(e) => updateContentType(e.target.value)}
                className="text-sm border border-gray-300 rounded-md px-2 py-1 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              >
                {availableContentTypes.map(contentType => (
                  <option key={contentType} value={contentType}>
                    {contentType}
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>
        
        <div className="space-y-2">
          <textarea
            value={formState.body}
            onChange={(e) => updateBody(e.target.value)}
            placeholder={placeholder}
            className={`w-full h-64 px-3 py-2 border rounded-md font-mono text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 ${
              bodyError ? 'border-red-300' : 'border-gray-300'
            }`}
          />
          {bodyError && (
            <div className="text-red-600 text-sm">{bodyError}</div>
          )}
          {selectedContentType && (
            <div className="text-xs text-gray-500">
              Content-Type: <code className="bg-gray-100 px-1 rounded">{selectedContentType}</code>
              {isJSONContentType(selectedContentType) && " (JSON format required)"}
            </div>
          )}
        </div>
      </div>
    )
  }

  const renderAuth = () => {
    return (
      <div className="space-y-4">
        <h4 className="font-medium text-gray-900">Authentication</h4>
        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Authentication Type
            </label>
            <select
              value={formState.auth.type}
              onChange={(e) => updateAuth(e.target.value as any)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              {AUTH_TYPES.map(type => (
                <option key={type.value} value={type.value}>
                  {type.label}
                </option>
              ))}
            </select>
          </div>

          {formState.auth.type !== 'none' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                {formState.auth.type === 'bearer' && 'Bearer Token'}
                {formState.auth.type === 'apikey' && 'API Key'}
                {formState.auth.type === 'basic' && 'Basic Auth (base64 encoded)'}
              </label>
              <input
                type="password"
                value={formState.auth.value}
                onChange={(e) => updateAuth(formState.auth.type, e.target.value)}
                placeholder={
                  formState.auth.type === 'bearer' ? 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...' :
                  formState.auth.type === 'apikey' ? 'your-api-key-here' :
                  'dXNlcm5hbWU6cGFzc3dvcmQ='
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
          )}
        </div>
      </div>
    )
  }

  return (
    <div className="bg-white border border-gray-200 rounded-lg">
      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="flex space-x-8 px-6">
          {(['params', 'headers', 'body', 'auth'] as const).map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`py-4 px-1 border-b-2 font-medium text-sm capitalize ${
                activeTab === tab
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab === 'params' ? 'Parameters' : tab}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="p-6">
        {activeTab === 'params' && renderParameters()}
        {activeTab === 'headers' && renderHeaders()}
        {activeTab === 'body' && renderBody()}
        {activeTab === 'auth' && renderAuth()}
      </div>

      {/* Execute Button */}
      <div className="border-t border-gray-200 px-6 py-4">
        <button
          onClick={handleExecute}
          disabled={isLoading || !!bodyError}
          className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isLoading ? (
            <>
              <div className="animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent"></div>
              Executing...
            </>
          ) : (
            <>
              <PaperAirplaneIcon className="h-4 w-4" />
              Send Request
            </>
          )}
        </button>
      </div>
    </div>
  )
}

// Parameter Input Component
interface ParameterInputProps {
  parameter: Parameter
  value: string
  onChange: (value: string) => void
}

const ParameterInput: React.FC<ParameterInputProps> = ({ parameter, value, onChange }) => {
  return (
    <div className="space-y-1">
      <label className="block text-sm font-medium text-gray-700">
        {parameter.name}
        {parameter.required && <span className="text-red-500 ml-1">*</span>}
      </label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={parameter.example?.toString() || `Enter ${parameter.name}`}
        required={parameter.required}
        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
      />
      {parameter.description && (
        <div className="text-xs text-gray-500">{parameter.description}</div>
      )}
    </div>
  )
}

export default RequestBuilder