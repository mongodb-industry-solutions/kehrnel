import { useState, useEffect } from 'react'
import { APIResource } from '@/types/openapi'
import { getAPIResources } from '@/services/openapi'

interface UseAPIResourcesReturn {
  resources: APIResource[]
  loading: boolean
  error: string | null
  retry: () => void
}

/**
 * Custom hook to fetch and manage API resources from OpenAPI spec
 */
export function useAPIResources(): UseAPIResourcesReturn {
  const [resources, setResources] = useState<APIResource[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchResources = async () => {
    try {
      setLoading(true)
      setError(null)
      const apiResources = await getAPIResources()
      setResources(apiResources)
    } catch (err) {
      console.error('Error fetching API resources:', err)
      setError(err instanceof Error ? err.message : 'Failed to fetch API resources')
      // Fallback to empty array instead of keeping old data
      setResources([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchResources()
  }, [])

  const retry = () => {
    fetchResources()
  }

  return {
    resources,
    loading,
    error,
    retry
  }
}