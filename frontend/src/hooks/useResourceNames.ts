import { useState, useEffect } from 'react'
import { getResourceNames } from '@/services/openapi'

interface UseResourceNamesReturn {
  resourceNames: string[]
  loading: boolean
  error: string | null
}

/**
 * Custom hook to fetch resource names for navigation
 */
export function useResourceNames(): UseResourceNamesReturn {
  const [resourceNames, setResourceNames] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchResourceNames = async () => {
      try {
        setLoading(true)
        setError(null)
        const names = await getResourceNames()
        setResourceNames(names)
      } catch (err) {
        console.error('Error fetching resource names:', err)
        setError(err instanceof Error ? err.message : 'Failed to fetch resource names')
        // Fallback to empty array
        setResourceNames([])
      } finally {
        setLoading(false)
      }
    }

    fetchResourceNames()
  }, [])

  return {
    resourceNames,
    loading,
    error
  }
}