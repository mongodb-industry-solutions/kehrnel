'use client'

import { useState } from 'react'
import { CheckCircleIcon, XCircleIcon, ClockIcon, DocumentDuplicateIcon } from '@heroicons/react/24/outline'
import { APIResponse, getStatusCodeInfo, formatBytes, formatDuration } from '../types/apiTesting'
import CodeBlock from './CodeBlock'

interface ResponseViewerProps {
  response: APIResponse | null
  isLoading: boolean
}

const ResponseViewer: React.FC<ResponseViewerProps> = ({ response, isLoading }) => {
  const [activeTab, setActiveTab] = useState<'body' | 'headers' | 'info'>('body')
  const [copied, setCopied] = useState(false)

  if (isLoading) {
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-8">
        <div className="flex items-center justify-center">
          <div className="animate-spin rounded-full h-8 w-8 border-2 border-blue-600 border-t-transparent"></div>
          <span className="ml-3 text-gray-600">Executing request...</span>
        </div>
      </div>
    )
  }

  if (!response) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-8">
        <div className="text-center text-gray-500">
          <div className="text-lg font-medium mb-2">No Response Yet</div>
          <div className="text-sm">Configure your request and click "Send Request" to see the response here.</div>
        </div>
      </div>
    )
  }

  const statusInfo = getStatusCodeInfo(response.status)

  const copyToClipboard = () => {
    const content = typeof response.data === 'string' 
      ? response.data 
      : JSON.stringify(response.data, null, 2)
    
    navigator.clipboard.writeText(content).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  const renderStatusBadge = () => {
    const bgColor = statusInfo.type === 'success' 
      ? 'bg-green-100 text-green-800'
      : statusInfo.type === 'error'
      ? 'bg-red-100 text-red-800'
      : statusInfo.type === 'warning'
      ? 'bg-yellow-100 text-yellow-800'
      : 'bg-blue-100 text-blue-800'

    const icon = response.success ? (
      <CheckCircleIcon className="h-4 w-4" />
    ) : (
      <XCircleIcon className="h-4 w-4" />
    )

    return (
      <div className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-sm font-medium ${bgColor}`}>
        {icon}
        {response.status} {response.statusText}
      </div>
    )
  }

  const renderBody = () => {
    if (!response.data) {
      return (
        <div className="text-gray-500 text-sm p-4">
          No response body
        </div>
      )
    }

    // Format the data for display
    const formattedData = typeof response.data === 'string' 
      ? response.data 
      : JSON.stringify(response.data, null, 2)

    const language = typeof response.data === 'object' ? 'json' : 'text'

    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h4 className="font-medium text-gray-900">Response Body</h4>
          <button
            onClick={copyToClipboard}
            className="text-blue-600 hover:text-blue-700 text-sm flex items-center gap-1"
          >
            <DocumentDuplicateIcon className="h-4 w-4" />
            {copied ? 'Copied!' : 'Copy'}
          </button>
        </div>
        
        <div className="border rounded-md overflow-hidden">
          <CodeBlock code={formattedData} language={language} />
        </div>
      </div>
    )
  }

  const renderHeaders = () => {
    return (
      <div className="space-y-4">
        <h4 className="font-medium text-gray-900">Response Headers</h4>
        
        {Object.keys(response.headers).length === 0 ? (
          <div className="text-gray-500 text-sm p-4">
            No response headers
          </div>
        ) : (
          <div className="space-y-2">
            {Object.entries(response.headers).map(([name, value]) => (
              <div key={name} className="flex">
                <div className="w-1/3 px-3 py-2 bg-gray-50 border border-gray-300 rounded-l-md font-mono text-sm font-medium">
                  {name}
                </div>
                <div className="flex-1 px-3 py-2 border-t border-r border-b border-gray-300 rounded-r-md font-mono text-sm">
                  {value}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    )
  }

  const renderInfo = () => {
    return (
      <div className="space-y-6">
        <div>
          <h4 className="font-medium text-gray-900 mb-3">Request Information</h4>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-600">Status:</span>
              <div>{renderStatusBadge()}</div>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Response Time:</span>
              <span className="font-mono text-sm">{formatDuration(response.duration)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Response Size:</span>
              <span className="font-mono text-sm">{formatBytes(response.size)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Timestamp:</span>
              <span className="font-mono text-sm">
                {new Date(response.timestamp).toLocaleString()}
              </span>
            </div>
          </div>
        </div>

        <div>
          <h4 className="font-medium text-gray-900 mb-3">Status Code Information</h4>
          <div className="p-4 bg-gray-50 rounded-md">
            <div className="flex items-center gap-2 mb-2">
              {response.success ? (
                <CheckCircleIcon className="h-5 w-5 text-green-600" />
              ) : (
                <XCircleIcon className="h-5 w-5 text-red-600" />
              )}
              <span className="font-medium">{statusInfo.text}</span>
            </div>
            <div className="text-sm text-gray-600">
              {statusInfo.description}
            </div>
          </div>
        </div>

        {!response.success && response.data && (
          <div>
            <h4 className="font-medium text-gray-900 mb-3">Error Details</h4>
            <div className="p-4 bg-red-50 rounded-md">
              <div className="text-sm text-red-800">
                {typeof response.data === 'object' && response.data.error
                  ? response.data.error
                  : typeof response.data === 'string'
                  ? response.data
                  : 'An error occurred while processing the request'
                }
              </div>
            </div>
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="bg-white border border-gray-200 rounded-lg">
      {/* Header */}
      <div className="border-b border-gray-200 p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h3 className="text-lg font-medium text-gray-900">Response</h3>
            {renderStatusBadge()}
          </div>
          <div className="flex items-center gap-4 text-sm text-gray-500">
            <div className="flex items-center gap-1">
              <ClockIcon className="h-4 w-4" />
              {formatDuration(response.duration)}
            </div>
            <div>{formatBytes(response.size)}</div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="flex space-x-8 px-6">
          {(['body', 'headers', 'info'] as const).map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`py-4 px-1 border-b-2 font-medium text-sm capitalize ${
                activeTab === tab
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="p-6">
        {activeTab === 'body' && renderBody()}
        {activeTab === 'headers' && renderHeaders()}
        {activeTab === 'info' && renderInfo()}
      </div>
    </div>
  )
}

export default ResponseViewer