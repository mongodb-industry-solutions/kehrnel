'use client'

import { useState, useEffect } from 'react'
import { 
  ChevronLeftIcon, 
  ChevronRightIcon, 
  PlayIcon, 
  BookOpenIcon, 
  AcademicCapIcon,
  SparklesIcon,
  ClockIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline'
import { aqlExamples, difficultyColors, difficultyLabels, type AQLExample } from '@/data/aqlExamples'
import CodeBlock from './CodeBlock'

const AQLPlayground = () => {
  const [currentQueryIndex, setCurrentQueryIndex] = useState(0)
  const [editableQuery, setEditableQuery] = useState('')
  const [isModified, setIsModified] = useState(false)
  const [isExecuting, setIsExecuting] = useState(false)
  const [executionResult, setExecutionResult] = useState<any>(null)
  const [executionError, setExecutionError] = useState<string | null>(null)

  const currentExample = aqlExamples[currentQueryIndex]
  const difficulty = difficultyColors[currentExample.difficulty]

  // Update editable query when example changes
  useEffect(() => {
    setEditableQuery(currentExample.query)
    setIsModified(false)
    setExecutionResult(null)
    setExecutionError(null)
  }, [currentQueryIndex, currentExample.query])

  // Check if query has been modified
  useEffect(() => {
    setIsModified(editableQuery.trim() !== currentExample.query.trim())
  }, [editableQuery, currentExample.query])

  const nextQuery = () => {
    if (currentQueryIndex < aqlExamples.length - 1) {
      setCurrentQueryIndex(currentQueryIndex + 1)
    }
  }

  const previousQuery = () => {
    if (currentQueryIndex > 0) {
      setCurrentQueryIndex(currentQueryIndex - 1)
    }
  }

  const resetQuery = () => {
    setEditableQuery(currentExample.query)
    setIsModified(false)
    setExecutionResult(null)
    setExecutionError(null)
  }

  const executeQuery = async () => {
    setIsExecuting(true)
    setExecutionError(null)
    
    try {
      // Connect to actual AQL API endpoint
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:9000'}/v1/query/aql`, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/plain',
        },
        body: editableQuery,
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const result = await response.json()
      setExecutionResult(result)
    } catch (error) {
      console.error('Query execution error:', error)
      setExecutionError(error instanceof Error ? error.message : 'Unknown error occurred')
    } finally {
      setIsExecuting(false)
    }
  }

  const progress = ((currentQueryIndex + 1) / aqlExamples.length) * 100

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
      {/* Header */}
      <div className="bg-white/80 backdrop-blur-sm border-b border-gray-200 sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <SparklesIcon className="h-6 w-6 text-purple-600" />
                <h1 className="text-xl font-bold text-gray-900">AQL Playground</h1>
              </div>
            <div className="hidden sm:flex items-center space-x-2 text-sm text-gray-500">
              <span>Showcase AQL Features & Capabilities</span>
              <span className="w-1 h-1 bg-gray-300 rounded-full"></span>
              <span>Supported Query Types</span>
            </div>
            </div>
            
            {/* Progress indicator */}
            <div className="flex items-center space-x-3">
              <div className="hidden sm:block text-sm text-gray-600">
                Query {currentQueryIndex + 1} of {aqlExamples.length}
              </div>
              <div className="w-32 bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-gradient-to-r from-blue-500 to-indigo-600 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${progress}%` }}
                />
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          
          {/* Left Sidebar - Query Info */}
          <div className="lg:col-span-1 space-y-6">
            
            {/* Query Title and Difficulty */}
            <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg p-6">
              <div className="flex items-start justify-between mb-4">
                <div className="flex-1">
                  <h2 className="text-lg font-semibold text-gray-900 mb-2">
                    {currentExample.title}
                  </h2>
                  <div className={`inline-flex items-center px-3 py-1 rounded-full text-xs font-medium ${difficulty.bg} ${difficulty.text} ${difficulty.border} border`}>
                    <AcademicCapIcon className={`h-3 w-3 mr-1 ${difficulty.icon}`} />
                    {difficultyLabels[currentExample.difficulty]}
                  </div>
                </div>
              </div>
              
              <p className="text-gray-600 text-sm leading-relaxed">
                {currentExample.description}
              </p>
            </div>

            {/* Concepts */}
            <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg p-6">
              <h3 className="text-sm font-medium text-gray-900 mb-3 flex items-center">
                <BookOpenIcon className="h-4 w-4 mr-2 text-blue-600" />
                Key Concepts
              </h3>
              <div className="flex flex-wrap gap-2">
                {currentExample.concepts.map((concept, index) => (
                  <span 
                    key={index}
                    className="inline-flex items-center px-2 py-1 rounded-md text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200"
                  >
                    {concept}
                  </span>
                ))}
              </div>
            </div>

            {/* Expected Result */}
            {currentExample.expectedResult && (
              <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg p-6">
                <h3 className="text-sm font-medium text-gray-900 mb-3 flex items-center">
                  <CheckCircleIcon className="h-4 w-4 mr-2 text-green-600" />
                  Expected Result
                </h3>
                <p className="text-sm text-gray-600 leading-relaxed">
                  {currentExample.expectedResult}
                </p>
              </div>
            )}

            {/* Tips */}
            {currentExample.tips && currentExample.tips.length > 0 && (
              <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg p-6">
                <h3 className="text-sm font-medium text-gray-900 mb-3">
                  💡 Pro Tips
                </h3>
                <ul className="space-y-2">
                  {currentExample.tips.map((tip, index) => (
                    <li key={index} className="text-sm text-gray-600 flex items-start">
                      <span className="flex-shrink-0 w-1.5 h-1.5 bg-purple-400 rounded-full mt-2 mr-3"></span>
                      <span>{tip}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          {/* Main Content - Query Editor and Results */}
          <div className="lg:col-span-2 space-y-6">
            
            {/* Query Editor */}
            <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg">
              <div className="flex items-center justify-between p-4 border-b border-gray-200">
                <h3 className="text-lg font-medium text-gray-900">Interactive Query Editor</h3>
                <div className="flex items-center space-x-2">
                  {isModified && (
                    <button
                      onClick={resetQuery}
                      className="text-sm text-gray-500 hover:text-gray-700 transition-colors"
                    >
                      Reset
                    </button>
                  )}
                  <button
                    onClick={executeQuery}
                    disabled={isExecuting}
                    className="flex items-center space-x-2 px-4 py-2 bg-gradient-to-r from-blue-500 to-indigo-600 text-white rounded-lg hover:from-blue-600 hover:to-indigo-700 hover:shadow-lg disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
                  >
                    {isExecuting ? (
                      <>
                        <ClockIcon className="h-4 w-4 animate-spin" />
                        <span>Running...</span>
                      </>
                    ) : (
                      <>
                        <PlayIcon className="h-4 w-4" />
                        <span>Run Query</span>
                      </>
                    )}
                  </button>
                </div>
              </div>
              
              <div className="p-4">
                <textarea
                  value={editableQuery}
                  onChange={(e) => setEditableQuery(e.target.value)}
                  className="w-full h-48 p-4 font-mono text-sm bg-gray-50 border border-gray-200 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent resize-none"
                  placeholder="Enter your AQL query here..."
                />
                {isModified && (
                  <div className="mt-2 text-xs text-amber-600 flex items-center">
                    <ExclamationTriangleIcon className="h-3 w-3 mr-1" />
                    Query has been modified from the original example
                  </div>
                )}
              </div>
            </div>

            {/* Features Supported */}
            <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Features Supported</h3>
              <div className="prose prose-sm max-w-none">
                <div className="text-gray-600 leading-relaxed space-y-3">
                  {currentExample.explanation.split('\n').map((line, index) => {
                    // Handle empty lines
                    if (line.trim() === '') {
                      return <div key={index} className="h-2"></div>
                    }
                    
                    // Handle bullet points with bold text
                    if (line.includes('• **') && line.includes('**:')) {
                      const parts = line.split('**')
                      return (
                        <div key={index} className="flex items-start space-x-2">
                          <span className="text-purple-500 font-bold mt-0.5">•</span>
                          <div>
                            <span className="font-semibold text-gray-800">{parts[1]}</span>
                            <span className="text-gray-600">: {parts[2].substring(1)}</span>
                          </div>
                        </div>
                      )
                    }
                    
                    // Handle regular text
                    return (
                      <p key={index} className="text-gray-600">
                        {line}
                      </p>
                    )
                  })}
                </div>
              </div>
            </div>

            {/* Results Section */}
            {(executionResult || executionError) && (
              <div className="bg-white/80 backdrop-blur-sm rounded-2xl border border-white/20 shadow-lg">
                <div className="p-4 border-b border-gray-200">
                  <h3 className="text-lg font-medium text-gray-900">Query Results</h3>
                </div>
                <div className="p-4">
                  {executionError ? (
                    <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
                      <div className="flex items-center mb-2">
                        <ExclamationTriangleIcon className="h-5 w-5 text-red-500 mr-2" />
                        <span className="font-medium text-red-800">Query Error</span>
                      </div>
                      <pre className="text-sm text-red-700 whitespace-pre-wrap">{executionError}</pre>
                    </div>
                  ) : (
                    <div className="max-h-96 overflow-y-auto">
                      <CodeBlock
                        code={JSON.stringify(executionResult, null, 2)}
                        language="json"
                      />
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Navigation */}
        <div className="mt-12 flex items-center justify-between">
          <button
            onClick={previousQuery}
            disabled={currentQueryIndex === 0}
            className="flex items-center space-x-2 px-6 py-3 bg-white/80 backdrop-blur-sm text-gray-700 border border-gray-200 rounded-xl hover:bg-white hover:shadow-lg disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
          >
            <ChevronLeftIcon className="h-4 w-4" />
            <span>Previous Query</span>
          </button>

          <div className="flex items-center space-x-2">
            {aqlExamples.map((_, index) => (
              <button
                key={index}
                onClick={() => setCurrentQueryIndex(index)}
                className={`w-3 h-3 rounded-full transition-all duration-200 ${
                  index === currentQueryIndex
                    ? 'bg-gradient-to-r from-blue-500 to-indigo-600'
                    : 'bg-gray-300 hover:bg-gray-400'
                }`}
              />
            ))}
          </div>

          <button
            onClick={nextQuery}
            disabled={currentQueryIndex === aqlExamples.length - 1}
            className="flex items-center space-x-2 px-6 py-3 bg-white/80 backdrop-blur-sm text-gray-700 border border-gray-200 rounded-xl hover:bg-white hover:shadow-lg disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
          >
            <span>Next Query</span>
            <ChevronRightIcon className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  )
}

export default AQLPlayground