'use client'

import { useState } from 'react'
import { ChevronDownIcon, ChevronRightIcon, ExclamationTriangleIcon, BeakerIcon, ClockIcon } from '@heroicons/react/24/outline'
import CodeBlock from './CodeBlock'
import RequestBuilder from './RequestBuilder'
import ResponseViewer from './ResponseViewer'
import RequestHistory from './RequestHistory'
import RequestHistoryFull from './RequestHistoryFull'
import { useAPIResources } from '@/hooks/useAPIResources'
import { useAPITesting } from '../hooks/useAPITesting'
import { formatDuration } from '../types/apiTesting'
import { APIResource, APIOperation, Parameter, RequestBody, Response, Example } from '@/types/openapi'

const APIExplorerWithSidebar = () => {
  const { resources, loading, error, retry } = useAPIResources()
  const { testState, history, executeRequest, loadFromHistory, clearHistory, resetTest } = useAPITesting()
  
  const [selectedResource, setSelectedResource] = useState<string>('')
  const [expandedOperations, setExpandedOperations] = useState<{ [key: string]: boolean }>({})
  const [selectedTab, setSelectedTab] = useState<{ [key: string]: string }>({})
  const [selectedHistoryId, setSelectedHistoryId] = useState<string>('')
  const [showFullHistory, setShowFullHistory] = useState(false)

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
      case 'GET': return 'bg-blue-100 text-blue-800 border-blue-200'
      case 'POST': return 'bg-green-100 text-green-800 border-green-200'
      case 'PUT': return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'DELETE': return 'bg-red-100 text-red-800 border-red-200'
      default: return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const handleHistorySelect = (historyItem: any) => {
    loadFromHistory(historyItem)
    setSelectedHistoryId(historyItem.id)
  }

  const handleResourceSelect = (resourceName: string) => {
    setSelectedResource(resourceName)
    // Reset expanded operations when switching resources
    setExpandedOperations({})
    setSelectedTab({})
  }

  // Enhanced executeRequest that automatically switches to response tab
  const handleExecuteRequest = async (request: any, operationKey: string) => {
    try {
      // Execute the request
      await executeRequest(request)
      // Add a small delay for better UX, then switch to response tab
      setTimeout(() => {
        setTabForOperation(operationKey, 'response')
      }, 100)
    } catch (error) {
      // Even on error, show the response tab to display the error
      setTimeout(() => {
        setTabForOperation(operationKey, 'response')
      }, 100)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center p-8 min-h-screen">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-blue-600 border-t-transparent"></div>
        <span className="ml-3 text-gray-600">Loading API resources...</span>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center p-8 min-h-screen">
        <ExclamationTriangleIcon className="h-12 w-12 text-red-500 mb-4" />
        <h3 className="text-lg font-medium text-gray-900 mb-2">Failed to Load API Resources</h3>
        <p className="text-gray-600 mb-4 text-center max-w-md">
          Unable to fetch the OpenAPI specification from the backend. Please ensure the server is running on{' '}
          <code className="bg-gray-100 px-1 py-0.5 rounded text-sm">
            {process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:9000'}
          </code>
        </p>
        <button
          onClick={retry}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Retry
        </button>
      </div>
    )
  }

  if (resources.length === 0) {
    return (
      <div className="text-center p-8 min-h-screen flex items-center justify-center">
        <p className="text-gray-600">No API resources found.</p>
      </div>
    )
  }

  const currentResource = resources.find(r => r.name === selectedResource)

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Left Sidebar - Resource Navigation */}
      <div className="w-80 bg-white border-r border-gray-200 flex flex-col">
        {/* Sidebar Header */}
        <div className="p-6 border-b border-gray-200">
          <div className="flex items-center gap-2">
            <BeakerIcon className="h-6 w-6 text-blue-600" />
            <h2 className="text-lg font-semibold text-gray-900">API Resources</h2>
          </div>
          <p className="text-sm text-gray-500 mt-1">Select a resource to explore its operations</p>
        </div>

        {/* Resource List */}
        <div className="flex-1 overflow-y-auto">
          <nav className="p-4">
            <ul className="space-y-1">
              {resources.map((resource) => (
                <li key={resource.name}>
                  <button
                    onClick={() => handleResourceSelect(resource.name)}
                    className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                      selectedResource === resource.name
                        ? 'bg-blue-50 text-blue-700 border border-blue-200'
                        : 'text-gray-700 hover:bg-gray-50 hover:text-gray-900'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-semibold">{resource.name}</span>
                      <span className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded-full">
                        {resource.operations.length}
                      </span>
                    </div>
                    {resource.description && (
                      <div className="text-xs text-gray-500 mt-1 truncate">
                        {resource.description}
                      </div>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        {/* Request History in Sidebar */}
        <div className="border-t border-gray-200">
          <div className="p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-900">Recent Requests</h3>
              {history.length > 0 && (
                <button
                  onClick={() => setShowFullHistory(true)}
                  className="text-xs text-blue-600 hover:text-blue-700"
                >
                  View All
                </button>
              )}
            </div>
            {history.length === 0 ? (
              <div className="text-xs text-gray-500 flex items-center gap-1">
                <ClockIcon className="h-3 w-3" />
                No recent requests
              </div>
            ) : (
              <div className="space-y-1">
                {history.slice(0, 3).map((item) => (
                  <button
                    key={item.id}
                    onClick={() => handleHistorySelect(item)}
                    className={`w-full text-left p-2 rounded text-xs hover:bg-gray-50 transition-colors ${
                      item.id === selectedHistoryId ? 'bg-blue-50 border border-blue-200' : ''
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className={`px-1 py-0.5 rounded text-xs font-medium ${getMethodBadgeColor(item.request.method)}`}>
                        {item.request.method}
                      </span>
                      {item.response.success ? (
                        <span className="text-green-600">{item.response.status}</span>
                      ) : (
                        <span className="text-red-600">{item.response.status}</span>
                      )}
                    </div>
                    <div className="text-gray-600 truncate mt-1 font-mono">{item.request.path}</div>
                    <div className="text-gray-400 text-xs mt-1">
                      {formatDuration(item.response.duration)} • {new Date(item.timestamp).toLocaleTimeString()}
                    </div>
                  </button>
                ))}
                {history.length > 3 && (
                  <button
                    onClick={() => setShowFullHistory(true)}
                    className="w-full text-xs text-blue-600 hover:text-blue-700 text-center pt-2 border-t border-gray-100"
                  >
                    View all {history.length} requests
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Main Header */}
        <div className="bg-white border-b border-gray-200 p-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">
                {currentResource?.name || 'Select a Resource'}
              </h1>
              <p className="text-gray-600 mt-1">
                {currentResource?.description || 'Choose a resource from the sidebar to explore its API operations'}
              </p>
            </div>
            <div className="text-sm text-gray-500">
              {currentResource && `${currentResource.operations.length} operations`}
            </div>
          </div>
        </div>

        {/* Operations Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {currentResource ? (
            <div className="max-w-6xl space-y-6">
              {currentResource.operations.map((operation, index) => {
                const operationKey = `${currentResource.name}-${index}`
                const isExpanded = expandedOperations[operationKey]
                const currentTab = getTabForOperation(operationKey)

                return (
                  <div key={operationKey} className="bg-white rounded-lg shadow-sm border border-gray-200">
                    {/* Operation Header */}
                    <button
                      onClick={() => toggleOperation(operationKey)}
                      className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-center space-x-4">
                        <span className={`px-3 py-1 rounded-md text-sm font-bold border ${getMethodBadgeColor(operation.method)}`}>
                          {operation.method}
                        </span>
                        <code className="text-sm font-mono text-gray-900">{operation.path}</code>
                        <span className="text-sm text-gray-600">{operation.summary}</span>
                      </div>
                      {isExpanded ? (
                        <ChevronDownIcon className="h-5 w-5 text-gray-400" />
                      ) : (
                        <ChevronRightIcon className="h-5 w-5 text-gray-400" />
                      )}
                    </button>

                    {/* Operation Details */}
                    {isExpanded && (
                      <div className="border-t border-gray-200">
                        {/* Tabs */}
                        <div className="border-b border-gray-200 bg-gray-50">
                          <nav className="flex space-x-8 px-6">
                            {['overview', 'test', 'response'].map((tab) => (
                              <button
                                key={tab}
                                onClick={() => setTabForOperation(operationKey, tab)}
                                className={`py-4 px-1 border-b-2 font-medium text-sm capitalize transition-colors ${
                                  currentTab === tab
                                    ? 'border-blue-500 text-blue-600 bg-white -mb-px'
                                    : testState.isLoading && tab === 'response'
                                    ? 'border-transparent text-blue-600 hover:text-blue-700 hover:border-blue-300'
                                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                                }`}
                              >
                                {tab === 'test' ? '🧪 Live Test' : tab === 'response' ? (
                                  <span className="flex items-center gap-1">
                                    📋 Response
                                    {testState.isLoading && (
                                      <div className="animate-spin rounded-full h-3 w-3 border-2 border-blue-600 border-t-transparent"></div>
                                    )}
                                  </span>
                                ) : '📖 Overview'}
                              </button>
                            ))}
                          </nav>
                        </div>

                        {/* Tab Content */}
                        <div className="p-6">
                          {currentTab === 'overview' && (
                            <OperationOverview operation={operation} />
                          )}
                          
                          {currentTab === 'test' && (
                            <RequestBuilder
                              operation={operation}
                              onExecute={(request) => handleExecuteRequest(request, operationKey)}
                              isLoading={testState.isLoading}
                            />
                          )}
                          
                          {currentTab === 'response' && (
                            <ResponseViewer
                              response={testState.response}
                              isLoading={testState.isLoading}
                            />
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          ) : (
            <div className="flex items-center justify-center h-64">
              <div className="text-center">
                <BeakerIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-gray-900 mb-2">Select a Resource</h3>
                <p className="text-gray-600">Choose a resource from the sidebar to explore its API operations</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Full History Modal */}
      {showFullHistory && (
        <RequestHistoryFull
          history={history}
          onSelectRequest={(item) => {
            handleHistorySelect(item)
            setShowFullHistory(false)
          }}
          onClearHistory={() => {
            clearHistory()
            setShowFullHistory(false)
          }}
          currentRequestId={selectedHistoryId}
          onClose={() => setShowFullHistory(false)}
        />
      )}
    </div>
  )
}

// Operation Overview Component (reused from previous implementation)
interface OperationOverviewProps {
  operation: APIOperation
}

const OperationOverview: React.FC<OperationOverviewProps> = ({ operation }) => {
  return (
    <div className="space-y-6">
      {/* Description */}
      {operation.description && (
        <div>
          <h4 className="text-sm font-medium text-gray-900 mb-2">Description</h4>
          <p className="text-sm text-gray-600">{operation.description}</p>
        </div>
      )}

      {/* Parameters */}
      {operation.parameters && operation.parameters.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-gray-900 mb-3">Parameters</h4>
          <div className="space-y-3">
            {operation.parameters.map((param, idx) => (
              <div key={idx} className="border border-gray-200 rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center space-x-2">
                    <code className="text-sm font-mono text-blue-600">{param.name}</code>
                    <span className="text-xs px-2 py-1 bg-gray-100 text-gray-600 rounded">
                      {param.in}
                    </span>
                    {param.required && (
                      <span className="text-xs px-2 py-1 bg-red-100 text-red-600 rounded">
                        required
                      </span>
                    )}
                  </div>
                  <span className="text-xs text-gray-500">{param.type}</span>
                </div>
                {param.description && (
                  <p className="text-sm text-gray-600 mb-2">{param.description}</p>
                )}
                {param.example && (
                  <div>
                    <span className="text-xs text-gray-500">Example: </span>
                    <code className="text-xs font-mono text-gray-900">{param.example}</code>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Request Body */}
      {operation.requestBody && (
        <div>
          <h4 className="text-sm font-medium text-gray-900 mb-3">Request Body</h4>
          <div className="border border-gray-200 rounded-lg p-4">
            <p className="text-sm text-gray-600 mb-3">{operation.requestBody.description}</p>
            {operation.requestBody.content?.['application/json']?.example && (
              <div>
                <span className="text-xs text-gray-500 mb-2 block">Example:</span>
                <CodeBlock
                  code={JSON.stringify(operation.requestBody.content['application/json'].example, null, 2)}
                  language="json"
                />
              </div>
            )}
          </div>
        </div>
      )}

      {/* Responses */}
      {operation.responses && operation.responses.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-gray-900 mb-3">Responses</h4>
          <div className="space-y-3">
            {operation.responses.map((response, idx) => (
              <div key={idx} className="border border-gray-200 rounded-lg p-4">
                <div className="flex items-center space-x-2 mb-2">
                  <span className="text-sm font-mono text-green-600">{response.status}</span>
                  <span className="text-sm text-gray-600">{response.description}</span>
                </div>
                {response.example && (
                  <div>
                    <span className="text-xs text-gray-500 mb-2 block">Example:</span>
                    <CodeBlock
                      code={JSON.stringify(response.example, null, 2)}
                      language="json"
                    />
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default APIExplorerWithSidebar