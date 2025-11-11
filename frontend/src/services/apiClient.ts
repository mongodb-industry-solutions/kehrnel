import { APIRequest, APIResponse, RequestHistory } from '../types/apiTesting'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:9000'

/**
 * HTTP Client for making actual API requests
 */
export class APIClient {
  private static instance: APIClient
  private requestHistory: RequestHistory[] = []

  static getInstance(): APIClient {
    if (!APIClient.instance) {
      APIClient.instance = new APIClient()
    }
    return APIClient.instance
  }

  /**
   * Execute an API request and return the response
   */
  async executeRequest(request: APIRequest): Promise<APIResponse> {
    const startTime = Date.now()
    
    try {
      // Build the full URL
      const url = this.buildURL(request.path, request.pathParams, request.queryParams)
      
      // Prepare headers
      const headers = this.buildHeaders(request.headers, request.auth)
      
      // Prepare request options
      const requestOptions: RequestInit = {
        method: request.method,
        headers,
        signal: AbortSignal.timeout(30000), // 30 second timeout
      }

      // Add body for non-GET requests
      if (request.body && request.method !== 'GET') {
        requestOptions.body = JSON.stringify(request.body)
      }

      // Make the request
      const response = await fetch(url, requestOptions)
      const endTime = Date.now()
      const duration = endTime - startTime

      // Parse response
      let responseData: any = null
      let responseText = ''
      
      try {
        responseText = await response.text()
        if (responseText) {
          responseData = JSON.parse(responseText)
        }
      } catch {
        // If JSON parsing fails, keep as text
        responseData = responseText
      }

      // Build response object
      const apiResponse: APIResponse = {
        status: response.status,
        statusText: response.statusText,
        headers: this.parseResponseHeaders(response.headers),
        data: responseData,
        duration,
        size: responseText.length,
        timestamp: new Date().toISOString(),
        success: response.ok
      }

      // Add to history
      this.addToHistory(request, apiResponse)

      return apiResponse

    } catch (error) {
      const endTime = Date.now()
      const duration = endTime - startTime

      // Handle network or other errors
      const errorResponse: APIResponse = {
        status: 0,
        statusText: 'Network Error',
        headers: {},
        data: {
          error: error instanceof Error ? error.message : 'Unknown error occurred',
          type: 'NetworkError'
        },
        duration,
        size: 0,
        timestamp: new Date().toISOString(),
        success: false
      }

      // Add to history
      this.addToHistory(request, errorResponse)

      return errorResponse
    }
  }

  /**
   * Build complete URL with path and query parameters
   */
  private buildURL(path: string, pathParams?: Record<string, string>, queryParams?: Record<string, string>): string {
    let url = `${API_BASE_URL}${path}`

    // Replace path parameters
    if (pathParams) {
      Object.entries(pathParams).forEach(([key, value]) => {
        url = url.replace(`{${key}}`, encodeURIComponent(value))
      })
    }

    // Add query parameters
    if (queryParams && Object.keys(queryParams).length > 0) {
      const searchParams = new URLSearchParams()
      Object.entries(queryParams).forEach(([key, value]) => {
        if (value !== undefined && value !== '') {
          searchParams.append(key, value)
        }
      })
      const queryString = searchParams.toString()
      if (queryString) {
        url += `?${queryString}`
      }
    }

    return url
  }

  /**
   * Build request headers
   */
  private buildHeaders(customHeaders?: Record<string, string>, auth?: { type: string; value: string }): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    }

    // Add custom headers
    if (customHeaders) {
      Object.entries(customHeaders).forEach(([key, value]) => {
        if (value.trim()) {
          headers[key] = value
        }
      })
    }

    // Add authentication
    if (auth && auth.value.trim()) {
      switch (auth.type) {
        case 'bearer':
          headers['Authorization'] = `Bearer ${auth.value}`
          break
        case 'apikey':
          headers['X-API-Key'] = auth.value
          break
        case 'basic':
          headers['Authorization'] = `Basic ${auth.value}`
          break
      }
    }

    return headers
  }

  /**
   * Parse response headers
   */
  private parseResponseHeaders(headers: Headers): Record<string, string> {
    const headerObj: Record<string, string> = {}
    headers.forEach((value, key) => {
      headerObj[key] = value
    })
    return headerObj
  }

  /**
   * Add request to history
   */
  private addToHistory(request: APIRequest, response: APIResponse): void {
    const historyEntry: RequestHistory = {
      id: crypto.randomUUID(),
      request,
      response,
      timestamp: new Date().toISOString()
    }

    this.requestHistory.unshift(historyEntry)
    
    // Keep only last 50 requests
    if (this.requestHistory.length > 50) {
      this.requestHistory = this.requestHistory.slice(0, 50)
    }

    // Save to localStorage
    try {
      localStorage.setItem('kehrnel_api_history', JSON.stringify(this.requestHistory))
    } catch (error) {
      console.warn('Failed to save request history to localStorage:', error)
    }
  }

  /**
   * Get request history
   */
  getHistory(): RequestHistory[] {
    return [...this.requestHistory]
  }

  /**
   * Load history from localStorage
   */
  loadHistory(): void {
    try {
      const saved = localStorage.getItem('kehrnel_api_history')
      if (saved) {
        this.requestHistory = JSON.parse(saved)
      }
    } catch (error) {
      console.warn('Failed to load request history from localStorage:', error)
      this.requestHistory = []
    }
  }

  /**
   * Clear request history
   */
  clearHistory(): void {
    this.requestHistory = []
    try {
      localStorage.removeItem('kehrnel_api_history')
    } catch (error) {
      console.warn('Failed to clear request history from localStorage:', error)
    }
  }

  /**
   * Get a specific request from history
   */
  getHistoryItem(id: string): RequestHistory | undefined {
    return this.requestHistory.find(item => item.id === id)
  }
}

export const apiClient = APIClient.getInstance()