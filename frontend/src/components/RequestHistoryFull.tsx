'use client'

import { useState } from 'react'
import { TrashIcon, ClockIcon, CheckCircleIcon, XCircleIcon, MagnifyingGlassIcon } from '@heroicons/react/24/outline'
import { RequestHistory, formatDuration, getStatusCodeInfo } from '../types/apiTesting'

interface RequestHistoryFullProps {
  history: RequestHistory[]
  onSelectRequest: (request: RequestHistory) => void
  onClearHistory: () => void
  currentRequestId?: string
  onClose: () => void
}

const RequestHistoryFull: React.FC<RequestHistoryFullProps> = ({
  history,
  onSelectRequest,
  onClearHistory,
  currentRequestId,
  onClose
}) => {
  const [searchQuery, setSearchQuery] = useState('')
  const [filterMethod, setFilterMethod] = useState<string>('all')
  const [filterStatus, setFilterStatus] = useState<string>('all')

  // Filter history based on search and filters
  const filteredHistory = history.filter(item => {
    const matchesSearch = 
      item.request.path.toLowerCase().includes(searchQuery.toLowerCase()) ||
      item.request.method.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (item.response.data && JSON.stringify(item.response.data).toLowerCase().includes(searchQuery.toLowerCase()))

    const matchesMethod = filterMethod === 'all' || item.request.method === filterMethod
    
    const matchesStatus = 
      filterStatus === 'all' ||
      (filterStatus === 'success' && item.response.success) ||
      (filterStatus === 'error' && !item.response.success)

    return matchesSearch && matchesMethod && matchesStatus
  })

  const getMethodBadgeColor = (method: string) => {
    switch (method) {
      case 'GET': return 'bg-blue-100 text-blue-800 border-blue-200'
      case 'POST': return 'bg-green-100 text-green-800 border-green-200'
      case 'PUT': return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'DELETE': return 'bg-red-100 text-red-800 border-red-200'
      default: return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const methods = ['all', ...Array.from(new Set(history.map(item => item.request.method)))]

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-6xl h-5/6 flex flex-col">
        {/* Header */}
        <div className="border-b border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold text-gray-900">Request History</h2>
            <div className="flex items-center gap-2">
              <button
                onClick={onClearHistory}
                className="text-red-600 hover:text-red-700 text-sm flex items-center gap-1"
              >
                <TrashIcon className="h-4 w-4" />
                Clear All
              </button>
              <button
                onClick={onClose}
                className="text-gray-400 hover:text-gray-600"
              >
                <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>

          {/* Filters */}
          <div className="flex gap-4 items-center">
            {/* Search */}
            <div className="flex-1 relative">
              <MagnifyingGlassIcon className="h-5 w-5 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search requests..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            {/* Method Filter */}
            <select
              value={filterMethod}
              onChange={(e) => setFilterMethod(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              {methods.map(method => (
                <option key={method} value={method}>
                  {method === 'all' ? 'All Methods' : method}
                </option>
              ))}
            </select>

            {/* Status Filter */}
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="all">All Status</option>
              <option value="success">Success</option>
              <option value="error">Error</option>
            </select>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-hidden">
          {filteredHistory.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              <div className="text-center">
                <ClockIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-gray-900 mb-2">
                  {history.length === 0 ? 'No Request History' : 'No Matching Requests'}
                </h3>
                <p className="text-gray-600">
                  {history.length === 0 
                    ? 'Your API requests will appear here' 
                    : 'Try adjusting your search or filters'
                  }
                </p>
              </div>
            </div>
          ) : (
            <div className="h-full overflow-y-auto">
              <table className="w-full">
                <thead className="bg-gray-50 sticky top-0">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Request
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Duration
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Time
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {filteredHistory.map((item) => (
                    <tr
                      key={item.id}
                      onClick={() => onSelectRequest(item)}
                      className={`cursor-pointer hover:bg-gray-50 transition-colors ${
                        item.id === currentRequestId ? 'bg-blue-50 border-l-4 border-l-blue-500' : ''
                      }`}
                    >
                      <td className="px-6 py-4">
                        <div className="flex items-center space-x-3">
                          <span className={`px-2 py-1 rounded text-xs font-medium border ${getMethodBadgeColor(item.request.method)}`}>
                            {item.request.method}
                          </span>
                          <div>
                            <div className="font-mono text-sm text-gray-900">{item.request.path}</div>
                            {Object.keys(item.request.pathParams || {}).length > 0 && (
                              <div className="text-xs text-gray-500">
                                Params: {Object.entries(item.request.pathParams || {})
                                  .slice(0, 2)
                                  .map(([key, value]) => `${key}=${value}`)
                                  .join(', ')}
                                {Object.keys(item.request.pathParams || {}).length > 2 && '...'}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center space-x-2">
                          {item.response.success ? (
                            <CheckCircleIcon className="h-4 w-4 text-green-500" />
                          ) : (
                            <XCircleIcon className="h-4 w-4 text-red-500" />
                          )}
                          <span className={`text-sm font-medium ${
                            item.response.success ? 'text-green-600' : 'text-red-600'
                          }`}>
                            {item.response.status}
                          </span>
                          <span className="text-xs text-gray-500">
                            {item.response.statusText}
                          </span>
                        </div>
                        {!item.response.success && item.response.data?.error && (
                          <div className="text-xs text-red-600 mt-1 truncate max-w-xs">
                            {item.response.data.error}
                          </div>
                        )}
                      </td>
                      <td className="px-6 py-4">
                        <span className="text-sm font-mono text-gray-900">
                          {formatDuration(item.response.duration)}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <div className="text-sm text-gray-900">
                          {new Date(item.timestamp).toLocaleDateString()}
                        </div>
                        <div className="text-xs text-gray-500">
                          {new Date(item.timestamp).toLocaleTimeString()}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="border-t border-gray-200 p-4 bg-gray-50">
          <div className="flex items-center justify-between text-sm text-gray-500">
            <div>
              Showing {filteredHistory.length} of {history.length} requests
            </div>
            <div>
              Total requests: {history.length}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default RequestHistoryFull