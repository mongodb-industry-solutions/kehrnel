'use client'

import Link from 'next/link'
import { useState } from 'react'
import { ChevronDownIcon } from '@heroicons/react/24/outline'
import { useResourceNames } from '@/hooks/useResourceNames'

const Navigation = () => {
  const [isDocsOpen, setIsDocsOpen] = useState(false)
  const { resourceNames, loading: resourcesLoading } = useResourceNames()

  // Helper function to format resource name for URL
  const formatResourceForUrl = (resourceName: string) => {
    return resourceName.toLowerCase().replace(/[^a-z0-9]/g, '-')
  }

  // Helper function to format resource name for display
  const formatResourceForDisplay = (resourceName: string) => {
    // Add descriptive text for known resources
    const descriptions: { [key: string]: string } = {
      'AQL': 'AQL (Archetype Query Language)',
      'EHR': 'EHR (Electronic Health Record)',
      'EHR_STATUS': 'EHR Status',
      'Composition': 'Composition',
      'Contribution': 'Contribution',
      'Directory': 'Directory',
      'Ingest': 'Data Ingest',
      'Synthetic': 'Synthetic Data'
    }
    return descriptions[resourceName] || resourceName
  }

  return (
    <nav className="bg-white shadow-lg border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex items-center space-x-8">
            {/* Title/Logo */}
            <Link 
              href="https://github.com/Paco-Mateu/kehrnel" 
              target="_blank" 
              rel="noopener noreferrer"
              className="flex items-center space-x-2 text-xl font-bold text-blue-600 hover:text-blue-800 transition-colors"
            >
              <span>KEHRNEL</span>
            </Link>

            {/* Navigation Links */}
            <div className="hidden md:flex items-center space-x-6">
              {/* Docs Dropdown */}
              <div className="relative">
                <button
                  onClick={() => setIsDocsOpen(!isDocsOpen)}
                  className="flex items-center space-x-1 text-gray-700 hover:text-blue-600 px-3 py-2 text-sm font-medium transition-colors"
                >
                  <span>Docs</span>
                  <ChevronDownIcon className="h-4 w-4" />
                </button>
                
                {isDocsOpen && (
                  <div className="absolute top-full left-0 mt-1 w-64 bg-white rounded-md shadow-lg border border-gray-200 z-50">
                    <div className="py-1">
                      {resourcesLoading ? (
                        <div className="px-4 py-2 text-sm text-gray-500">
                          <div className="flex items-center">
                            <div className="animate-spin rounded-full h-3 w-3 border-b border-gray-400 mr-2"></div>
                            Loading resources...
                          </div>
                        </div>
                      ) : resourceNames.length > 0 ? (
                        resourceNames.map((resourceName) => (
                          <Link
                            key={resourceName}
                            href={`/docs/${formatResourceForUrl(resourceName)}`}
                            className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                            onClick={() => setIsDocsOpen(false)}
                          >
                            {formatResourceForDisplay(resourceName)}
                          </Link>
                        ))
                      ) : (
                        <div className="px-4 py-2 text-sm text-gray-500">
                          No resources available
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>

              {/* OpenEHR API */}
              <Link
                href="/api"
                className="text-gray-700 hover:text-blue-600 px-3 py-2 text-sm font-medium transition-colors"
              >
                OpenEHR API
              </Link>

              {/* Data Lab */}
              <Link
                href="#"
                target="_blank"
                rel="noopener noreferrer"
                className="text-gray-700 hover:text-blue-600 px-3 py-2 text-sm font-medium transition-colors"
              >
                Data Lab
              </Link>
            </div>
          </div>
        </div>
      </div>
    </nav>
  )
}

export default Navigation