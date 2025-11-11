import { useState, useEffect, useCallback } from 'react'
import { APIRequest, APIResponse, RequestHistory, TestState } from '../types/apiTesting'
import { apiClient } from '../services/apiClient'

export interface UseAPITestingReturn {
  testState: TestState
  history: RequestHistory[]
  executeRequest: (request: APIRequest) => Promise<void>
  loadFromHistory: (historyItem: RequestHistory) => void
  clearHistory: () => void
  resetTest: () => void
}

export function useAPITesting(): UseAPITestingReturn {
  const [testState, setTestState] = useState<TestState>({
    isLoading: false,
    request: null,
    response: null,
    error: null
  })

  const [history, setHistory] = useState<RequestHistory[]>([])

  // Load history from localStorage on mount
  useEffect(() => {
    apiClient.loadHistory()
    setHistory(apiClient.getHistory())
  }, [])

  const executeRequest = useCallback(async (request: APIRequest) => {
    setTestState(prev => ({
      ...prev,
      isLoading: true,
      request,
      response: null,
      error: null
    }))

    try {
      const response = await apiClient.executeRequest(request)
      
      setTestState(prev => ({
        ...prev,
        isLoading: false,
        response,
        error: response.success ? null : 'Request failed'
      }))

      // Update history
      setHistory(apiClient.getHistory())

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'
      
      setTestState(prev => ({
        ...prev,
        isLoading: false,
        response: null,
        error: errorMessage
      }))
    }
  }, [])

  const loadFromHistory = useCallback((historyItem: RequestHistory) => {
    setTestState({
      isLoading: false,
      request: historyItem.request,
      response: historyItem.response,
      error: null
    })
  }, [])

  const clearHistory = useCallback(() => {
    apiClient.clearHistory()
    setHistory([])
  }, [])

  const resetTest = useCallback(() => {
    setTestState({
      isLoading: false,
      request: null,
      response: null,
      error: null
    })
  }, [])

  return {
    testState,
    history,
    executeRequest,
    loadFromHistory,
    clearHistory,
    resetTest
  }
}