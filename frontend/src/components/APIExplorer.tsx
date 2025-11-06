'use client'

import { useState } from 'react'
import { ChevronDownIcon, ChevronRightIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline'
import CodeBlock from './CodeBlock'
import { useAPIResources } from '@/hooks/useAPIResources'
import { APIResource, APIOperation, Parameter, RequestBody, Response, Example } from '@/types/openapi'

const APIExplorer = () => {
  const { resources, loading, error, retry } = useAPIResources()
  const [selectedResource, setSelectedResource] = useState<string>('')
  const [expandedOperations, setExpandedOperations] = useState<{ [key: string]: boolean }>({})
  const [selectedTab, setSelectedTab] = useState<{ [key: string]: string }>({})

  // Set default selected resource when resources are loaded
  if (resources.length > 0 && !selectedResource) {
    setSelectedResource(resources[0].name)
  }

  const toggleOperation = (operationKey: string) => {
    setExpandedOperations(prev => ({
      ...prev,
      [operationKey]: !prev[operationKey]
    }))
  }

  const getTabForOperation = (operationKey: string) => {
    return selectedTab[operationKey] || 'overview'
  }

  const setTabForOperation = (operationKey: string, tab: string) => {
    setSelectedTab(prev => ({
      ...prev,
      [operationKey]: tab
    }))
  }

  const getMethodBadgeColor = (method: string) => {
    switch (method) {
      case 'GET': return 'bg-blue-100 text-blue-800'
      case 'POST': return 'bg-green-100 text-green-800'
      case 'PUT': return 'bg-yellow-100 text-yellow-800'
      case 'DELETE': return 'bg-red-100 text-red-800'
      default: return 'bg-gray-100 text-gray-800'
    }
  }

  // Loading state
  if (loading) {
    return (
      <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">API Explorer</h1>
          <p className="mt-2 text-gray-600">Loading API documentation from backend...</p>
        </div>
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <span className="ml-3 text-gray-600">Fetching API resources...</span>
        </div>
      </div>
    )
  }

  // Error state
  if (error) {
    return (
      <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">API Explorer</h1>
          <p className="mt-2 text-gray-600">Explore and test the KEHRNEL OpenEHR API endpoints</p>
        </div>
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <div className="flex items-center">
            <ExclamationTriangleIcon className="h-6 w-6 text-red-600 mr-3" />
            <div className="flex-1">
              <h3 className="text-lg font-medium text-red-800">Failed to Load API Resources</h3>
              <p className="mt-2 text-red-700">{error}</p>
              <p className="mt-2 text-sm text-red-600">
                Make sure your KEHRNEL backend is running at{' '}
                <code className="bg-red-100 px-1 py-0.5 rounded">
                  {process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:9000'}
                </code>
              </p>
            </div>
          </div>
          <div className="mt-4">
            <button
              onClick={retry}
              className="bg-red-600 text-white px-4 py-2 rounded-md hover:bg-red-700 transition-colors"
            >
              Retry Connection
            </button>
          </div>
        </div>
      </div>
    )
  }

  // No resources state
  if (resources.length === 0) {
    return (
      <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">API Explorer</h1>
          <p className="mt-2 text-gray-600">Explore and test the KEHRNEL OpenEHR API endpoints</p>
        </div>
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
          <div className="flex items-center">
            <ExclamationTriangleIcon className="h-6 w-6 text-yellow-600 mr-3" />
            <div>
              <h3 className="text-lg font-medium text-yellow-800">No API Resources Found</h3>
              <p className="mt-2 text-yellow-700">
                The backend API doesn't seem to have any exposed resources or the OpenAPI schema is empty.
              </p>
            </div>
          </div>
        </div>
      </div>
    )
  }

  const selectedResourceData = resources.find(r => r.name === selectedResource)

  return (
    <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">API Explorer</h1>
        <p className="mt-2 text-gray-600">
          Explore and test the KEHRNEL OpenEHR API endpoints dynamically loaded from your backend
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Resource List */}
        <div className="lg:col-span-1">
          <div className="bg-white rounded-lg shadow-md border border-gray-200">
            <div className="p-4 border-b border-gray-200">
              <h2 className="text-lg font-semibold text-gray-900">API Resources</h2>
              <p className="text-sm text-gray-600 mt-1">{resources.length} resources available</p>
            </div>
            <div className="p-4">
              <div className="space-y-2">
                {resources.map((resource) => (
                  <button
                    key={resource.name}
                    onClick={() => setSelectedResource(resource.name)}
                    className={`w-full text-left px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                      selectedResource === resource.name
                        ? 'bg-blue-100 text-blue-700 border border-blue-200'
                        : 'text-gray-700 hover:bg-gray-100'
                    }`}
                  >
                    <div className="font-medium">{resource.name}</div>
                    <div className="text-xs text-gray-500 mt-1">
                      {resource.operations.length} operation{resource.operations.length !== 1 ? 's' : ''}
                    </div>
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Resource Details */}
        <div className="lg:col-span-3">
          {selectedResourceData && (
            <div className="bg-white rounded-lg shadow-md border border-gray-200">
              <div className="p-6 border-b border-gray-200">
                <h2 className="text-2xl font-bold text-gray-900">{selectedResourceData.name}</h2>
                <p className="mt-2 text-gray-600">{selectedResourceData.description}</p>
              </div>

              <div className="p-6">
                <div className="space-y-4">
                  {selectedResourceData.operations.map((operation, index) => {
                    const operationKey = `${selectedResourceData.name}-${index}`
                    const isExpanded = expandedOperations[operationKey]
                    const currentTab = getTabForOperation(operationKey)

                    return (
                      <div key={operationKey} className="border border-gray-200 rounded-lg">
                        <button
                          onClick={() => toggleOperation(operationKey)}
                          className="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 transition-colors"
                        >
                          <div className="flex items-center space-x-3">
                            <span className={`px-3 py-1 rounded text-xs font-bold ${getMethodBadgeColor(operation.method)}`}>
                              {operation.method}
                            </span>
                            <code className="text-sm font-mono text-gray-800">{operation.path}</code>
                            <span className="text-gray-600">{operation.summary}</span>
                          </div>
                          {isExpanded ? (
                            <ChevronDownIcon className="h-5 w-5 text-gray-400" />
                          ) : (
                            <ChevronRightIcon className="h-5 w-5 text-gray-400" />
                          )}
                        </button>

                        {isExpanded && (
                          <div className="border-t border-gray-200 p-4">
                            <div className="mb-4">
                              <p className="text-gray-700">{operation.description}</p>
                            </div>

                            {/* Tabs */}
                            <div className="flex space-x-1 mb-4 border-b border-gray-200">
                              {['overview', 'parameters', 'request', 'examples', 'responses'].map((tab) => (
                                <button
                                  key={tab}
                                  onClick={() => setTabForOperation(operationKey, tab)}
                                  className={`px-3 py-2 text-sm font-medium capitalize transition-colors ${
                                    currentTab === tab
                                      ? 'text-blue-600 border-b-2 border-blue-600'
                                      : 'text-gray-500 hover:text-gray-700'
                                  }`}
                                >
                                  {tab}
                                </button>
                              ))}
                            </div>

                            {/* Tab Content */}
                            <div className="mt-4">
                              {currentTab === 'overview' && (
                                <div className="space-y-4">
                                  <div>
                                    <h4 className="font-medium text-gray-900 mb-2">Endpoint</h4>
                                    <div className="flex items-center space-x-2">
                                      <span className={`px-2 py-1 rounded text-xs font-medium ${getMethodBadgeColor(operation.method)}`}>
                                        {operation.method}
                                      </span>
                                      <code className="bg-gray-100 px-2 py-1 rounded text-sm">{operation.path}</code>
                                    </div>
                                  </div>
                                  <div>
                                    <h4 className="font-medium text-gray-900 mb-2">Description</h4>
                                    <p className="text-gray-700">{operation.description || operation.summary}</p>
                                  </div>
                                </div>
                              )}

                              {currentTab === 'parameters' && (
                                <div className="space-y-4">
                                  {operation.parameters && operation.parameters.length > 0 ? (
                                    operation.parameters.map((param, paramIndex) => (
                                      <div key={paramIndex} className="border border-gray-200 rounded-lg p-4">
                                        <div className="flex items-center space-x-2 mb-2">
                                          <span className="font-medium text-gray-900">{param.name}</span>
                                          <span className={`px-2 py-1 rounded text-xs ${
                                            param.required ? 'bg-red-100 text-red-800' : 'bg-gray-100 text-gray-800'
                                          }`}>
                                            {param.required ? 'required' : 'optional'}
                                          </span>
                                          <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-xs">
                                            {param.in}
                                          </span>
                                          <span className="px-2 py-1 bg-gray-100 text-gray-800 rounded text-xs">
                                            {param.type}
                                          </span>
                                        </div>
                                        <p className="text-gray-700 text-sm">{param.description}</p>
                                        {param.example && (
                                          <div className="mt-2">
                                            <span className="text-xs font-medium text-gray-500">Example:</span>
                                            <code className="ml-2 bg-gray-100 px-1 py-0.5 rounded text-xs">
                                              {param.example}
                                            </code>
                                          </div>
                                        )}
                                      </div>
                                    ))
                                  ) : (
                                    <p className="text-gray-500 italic">No parameters required</p>
                                  )}
                                </div>
                              )}

                              {currentTab === 'request' && (
                                <div className="space-y-4">
                                  {operation.requestBody ? (
                                    <div>
                                      <h4 className="font-medium text-gray-900 mb-2">Request Body</h4>
                                      <p className="text-gray-700 mb-4">{operation.requestBody.description}</p>
                                      {Object.entries(operation.requestBody.content).map(([contentType, content]) => (
                                        <div key={contentType} className="mb-4">
                                          <h5 className="font-medium text-gray-800 mb-2">{contentType}</h5>
                                          <CodeBlock
                                            language="json"
                                            code={JSON.stringify(content.example, null, 2)}
                                            title="Example Request"
                                          />
                                        </div>
                                      ))}
                                    </div>
                                  ) : (
                                    <p className="text-gray-500 italic">No request body required</p>
                                  )}
                                </div>
                              )}

                              {currentTab === 'examples' && (
                                <div className="space-y-6">
                                  {operation.examples.map((example, exampleIndex) => (
                                    <div key={exampleIndex} className="border border-gray-200 rounded-lg p-4">
                                      <h4 className="font-medium text-gray-900 mb-2">{example.title}</h4>
                                      <p className="text-gray-700 mb-4">{example.description}</p>
                                      
                                      {example.request && (
                                        <div className="mb-4">
                                          <CodeBlock
                                            language="json"
                                            code={example.request}
                                            title="Request"
                                          />
                                        </div>
                                      )}
                                      
                                      <CodeBlock
                                        language="json"
                                        code={JSON.stringify(example.response, null, 2)}
                                        title="Response"
                                      />
                                    </div>
                                  ))}
                                </div>
                              )}

                              {currentTab === 'responses' && (
                                <div className="space-y-4">
                                  {operation.responses.map((response, responseIndex) => (
                                    <div key={responseIndex} className="border border-gray-200 rounded-lg p-4">
                                      <div className="flex items-center space-x-2 mb-2">
                                        <span className={`px-2 py-1 rounded text-xs font-medium ${
                                          response.status.startsWith('2') ? 'bg-green-100 text-green-800' :
                                          response.status.startsWith('4') ? 'bg-red-100 text-red-800' :
                                          'bg-gray-100 text-gray-800'
                                        }`}>
                                          {response.status}
                                        </span>
                                        <span className="text-gray-900 font-medium">{response.description}</span>
                                      </div>
                                      <CodeBlock
                                        language="json"
                                        code={JSON.stringify(response.example, null, 2)}
                                      />
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default APIExplorer