'use client'

import { useState } from 'react'
import { TrashIcon, ClockIcon, CheckCircleIcon, XCircleIcon } from '@heroicons/react/24/outline'
import { RequestHistory, formatDuration, getStatusCodeInfo } from '../types/apiTesting'

interface RequestHistoryProps {
  history: RequestHistory[]
  onSelectRequest: (request: RequestHistory) => void
  onClearHistory: () => void
  currentRequestId?: string
}

const RequestHistoryComponent: React.FC<RequestHistoryProps> = ({
  history,
  onSelectRequest,
  onClearHistory,
  currentRequestId
}) => {
  const [isExpanded, setIsExpanded] = useState(false)

  if (history.length === 0) {
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-6">
        <div className="text-center text-gray-500">
          <ClockIcon className="h-8 w-8 mx-auto mb-2 text-gray-400" />
          <div className="text-sm">No request history</div>
          <div className="text-xs mt-1">Your API requests will appear here</div>
        </div>
      </div>
    )
  }

  const displayHistory = isExpanded ? history : history.slice(0, 5)

  return (
    <div className="bg-white border border-gray-200 rounded-lg">
      <div className="border-b border-gray-200 p-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-medium text-gray-900">
            Request History ({history.length})
          </h3>
          <button
            onClick={onClearHistory}
            className="text-red-600 hover:text-red-700 text-sm flex items-center gap-1"
          >
            <TrashIcon className="h-4 w-4" />
            Clear
          </button>
        </div>
      </div>

      <div className="divide-y divide-gray-200">
        {displayHistory.map((item) => (
          <HistoryItem
            key={item.id}
            item={item}
            onSelect={() => onSelectRequest(item)}
            isSelected={item.id === currentRequestId}
          />
        ))}
      </div>

      {history.length > 5 && (
        <div className="border-t border-gray-200 p-4">
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="w-full text-blue-600 hover:text-blue-700 text-sm font-medium"
          >
            {isExpanded ? 'Show Less' : `Show All (${history.length - 5} more)`}
          </button>
        </div>
      )}
    </div>
  )
}

interface HistoryItemProps {
  item: RequestHistory
  onSelect: () => void
  isSelected: boolean
}

const HistoryItem: React.FC<HistoryItemProps> = ({ item, onSelect, isSelected }) => {
  const statusInfo = getStatusCodeInfo(item.response.status)

  const getMethodBadgeColor = (method: string) => {
    switch (method) {
      case 'GET': return 'bg-blue-100 text-blue-800'
      case 'POST': return 'bg-green-100 text-green-800'
      case 'PUT': return 'bg-yellow-100 text-yellow-800'
      case 'DELETE': return 'bg-red-100 text-red-800'
      default: return 'bg-gray-100 text-gray-800'
    }
  }

  return (
    <button
      onClick={onSelect}
      className={`w-full text-left p-4 hover:bg-gray-50 focus:bg-gray-50 focus:outline-none transition-colors ${
        isSelected ? 'bg-blue-50 border-l-4 border-l-blue-500' : ''
      }`}
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className={`px-2 py-1 rounded text-xs font-medium ${getMethodBadgeColor(item.request.method)}`}>
            {item.request.method}
          </span>
          <span className="font-mono text-sm text-gray-900 truncate">
            {item.request.path}
          </span>
        </div>
        
        <div className="flex items-center gap-2">
          {item.response.success ? (
            <CheckCircleIcon className="h-4 w-4 text-green-500" />
          ) : (
            <XCircleIcon className="h-4 w-4 text-red-500" />
          )}
          <span className={`text-xs font-medium ${
            item.response.success ? 'text-green-600' : 'text-red-600'
          }`}>
            {item.response.status}
          </span>
        </div>
      </div>

      <div className="flex items-center justify-between text-xs text-gray-500">
        <div className="flex items-center gap-3">
          <span>{formatDuration(item.response.duration)}</span>
          <span>{new Date(item.timestamp).toLocaleTimeString()}</span>
        </div>
        
        {/* Path parameters preview */}
        {Object.keys(item.request.pathParams || {}).length > 0 && (
          <div className="flex items-center gap-1">
            <span className="text-gray-400">params:</span>
            <span className="font-mono">
              {Object.entries(item.request.pathParams || {}).slice(0, 2).map(([key, value]) => 
                `${key}=${value}`
              ).join(', ')}
              {Object.keys(item.request.pathParams || {}).length > 2 && '...'}
            </span>
          </div>
        )}
      </div>

      {/* Error preview */}
      {!item.response.success && item.response.data?.error && (
        <div className="mt-2 text-xs text-red-600 truncate">
          {item.response.data.error}
        </div>
      )}
    </button>
  )
}

export default RequestHistoryComponent