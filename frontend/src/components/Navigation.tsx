'use client'

import Link from 'next/link'
import { useState, useEffect, useRef } from 'react'
import { ChevronDownIcon, BeakerIcon, DocumentTextIcon, ServerIcon } from '@heroicons/react/24/outline'
import { useResourceNames } from '@/hooks/useResourceNames'

const Navigation = () => {
  const [isDocsOpen, setIsDocsOpen] = useState(false)
  const { resourceNames, loading: resourcesLoading } = useResourceNames()
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Close dropdown when clicking outside or pressing Escape
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsDocsOpen(false)
      }
    }

    const handleEscapeKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsDocsOpen(false)
      }
    }

    // Only add event listeners when dropdown is open
    if (isDocsOpen) {
      document.addEventListener('mousedown', handleClickOutside)
      document.addEventListener('keydown', handleEscapeKey)
    }

    // Cleanup event listeners
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleEscapeKey)
    }
  }, [isDocsOpen])

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

  // Categorize resources for better UX in dropdown
  const categorizeResources = (resources: string[]) => {
    const categories = {
      'Core OpenEHR': ['EHR', 'EHR_STATUS', 'AQL', 'Template', 'Definition - Template', 'Contributions', 'Compositions', 'Directory'],
      'Other': ['Ingestion', 'Synthetic Data']
    }

    const categorized: { [key: string]: string[] } = {}
    const uncategorized: string[] = []

    // Initialize categories
    Object.keys(categories).forEach(cat => {
      categorized[cat] = []
    })

    // Categorize resources
    resources.forEach(resource => {
      let placed = false
      Object.entries(categories).forEach(([category, items]) => {
        if (items.includes(resource)) {
          categorized[category].push(resource)
          placed = true
        }
      })
      if (!placed) {
        uncategorized.push(resource)
      }
    })

    // Add uncategorized items to "Other" if any exist
    if (uncategorized.length > 0) {
      if (categorized['Other']) {
        categorized['Other'] = [...categorized['Other'], ...uncategorized]
      } else {
        categorized['Other'] = uncategorized
      }
    }

    // Remove empty categories
    Object.keys(categorized).forEach(key => {
      if (categorized[key].length === 0) {
        delete categorized[key]
      }
    })

    return categorized
  }

  // Get icon for resource category
  const getCategoryIcon = (category: string) => {
    const icons: { [key: string]: any } = {
      'Core OpenEHR': ServerIcon,
      'Other': DocumentTextIcon
    }
    return icons[category] || ServerIcon
  }

  return (
    <nav className="bg-gradient-to-r from-slate-900 via-blue-900 to-indigo-900 shadow-2xl border-b border-blue-800/30 backdrop-blur-sm">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-20">
          <div className="flex items-center space-x-8">
            {/* Enhanced Logo */}
            <Link 
              href="https://github.com/Paco-Mateu/kehrnel" 
              target="_blank" 
              rel="noopener noreferrer"
              className="group flex items-center space-x-3 text-2xl font-bold text-white hover:text-blue-300 transition-all duration-300 transform hover:scale-105"
            >
              <div className="flex items-center justify-center w-10 h-10 bg-gradient-to-br from-blue-400 to-indigo-500 rounded-xl shadow-lg group-hover:shadow-xl group-hover:from-blue-300 group-hover:to-indigo-400 transition-all duration-300">
                <ServerIcon className="h-6 w-6 text-white" />
              </div>
              <span className="bg-gradient-to-r from-white to-blue-100 bg-clip-text text-transparent">KEHRNEL</span>
            </Link>

            {/* Navigation Links */}
            <div className="hidden md:flex items-center space-x-2">
              {/* Enhanced Docs Dropdown */}
              <div className="relative" ref={dropdownRef}>
                <button
                  onClick={() => setIsDocsOpen(!isDocsOpen)}
                  className={`group flex items-center space-x-2 px-4 py-2.5 text-sm font-medium rounded-xl transition-all duration-300 ${
                    isDocsOpen 
                      ? 'bg-white/10 text-white shadow-lg backdrop-blur-sm' 
                      : 'text-blue-100 hover:text-white hover:bg-white/10 hover:shadow-lg hover:backdrop-blur-sm'
                  }`}
                >
                  <DocumentTextIcon className="h-4 w-4" />
                  <span>Documentation</span>
                  <ChevronDownIcon className={`h-4 w-4 transition-all duration-300 ${
                    isDocsOpen ? 'rotate-180 text-blue-300' : 'group-hover:text-blue-300'
                  }`} />
                </button>
                
                {isDocsOpen && (
                  <div className="absolute top-full left-0 mt-2 w-80 bg-white/95 backdrop-blur-lg rounded-2xl shadow-2xl border border-white/20 z-50 overflow-hidden animate-in slide-in-from-top-2 duration-200">
                    <div className="p-2">
                      <div className="mb-3 px-3 py-2">
                        <h3 className="text-sm font-semibold text-gray-800 mb-1">API Resources</h3>
                        <p className="text-xs text-gray-600">Explore OpenEHR API documentation</p>
                      </div>
                      {resourcesLoading ? (
                        <div className="px-3 py-3 text-sm text-gray-500">
                          <div className="flex items-center">
                            <div className="animate-spin rounded-full h-4 w-4 border-2 border-blue-500 border-t-transparent mr-3"></div>
                            <span>Loading resources...</span>
                          </div>
                        </div>
                      ) : resourceNames.length > 0 ? (
                        <div className="max-h-96 overflow-y-auto">
                          {Object.entries(categorizeResources(resourceNames)).map(([category, resources]) => (
                            <div key={category} className="mb-4 last:mb-0">
                              <div className="px-3 py-1.5 mb-2">
                                <div className="flex items-center space-x-2">
                                  {(() => {
                                    const IconComponent = getCategoryIcon(category)
                                    return <IconComponent className="h-4 w-4 text-gray-500" />
                                  })()}
                                  <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                                    {category}
                                  </h4>
                                </div>
                              </div>
                              <div className="space-y-1">
                                {resources.map((resourceName) => (
                                  <Link
                                    key={resourceName}
                                    href={`/docs/${formatResourceForUrl(resourceName)}`}
                                    className="group flex items-center px-3 py-2 text-sm text-gray-700 hover:text-blue-600 hover:bg-blue-50/80 rounded-lg transition-all duration-200 ml-2"
                                    onClick={() => setIsDocsOpen(false)}
                                  >
                                    <div className="flex items-center justify-center w-6 h-6 bg-gradient-to-br from-blue-100 to-indigo-100 group-hover:from-blue-200 group-hover:to-indigo-200 rounded-md mr-3 transition-all duration-200">
                                      {(() => {
                                        const IconComponent = getCategoryIcon(category)
                                        return <IconComponent className="h-3 w-3 text-blue-600" />
                                      })()}
                                    </div>
                                    <div className="flex-1">
                                      <div className="font-medium">{resourceName}</div>
                                      <div className="text-xs text-gray-500 group-hover:text-blue-500">
                                        {formatResourceForDisplay(resourceName)}
                                      </div>
                                    </div>
                                  </Link>
                                ))}
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="px-3 py-3 text-sm text-gray-500 text-center">
                          <ServerIcon className="h-8 w-8 text-gray-300 mx-auto mb-2" />
                          <div>No resources available</div>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>

              {/* Enhanced OpenEHR API Link */}
              <Link
                href="/api"
                className="group flex items-center space-x-2 px-4 py-2.5 text-sm font-medium rounded-xl text-blue-100 hover:text-white hover:bg-white/10 hover:shadow-lg hover:backdrop-blur-sm transition-all duration-300"
              >
                <BeakerIcon className="h-4 w-4 group-hover:text-blue-300 transition-colors duration-300" />
                <span>API Explorer</span>
              </Link>

              {/* Enhanced Data Lab Link */}
              <Link
                href="#"
                target="_blank"
                rel="noopener noreferrer"
                className="group flex items-center space-x-2 px-4 py-2.5 text-sm font-medium rounded-xl text-blue-100 hover:text-white hover:bg-white/10 hover:shadow-lg hover:backdrop-blur-sm transition-all duration-300"
              >
                <DocumentTextIcon className="h-4 w-4 group-hover:text-blue-300 transition-colors duration-300" />
                <span>Data Lab</span>
              </Link>
            </div>
          </div>

          {/* Right Side - Optional Status Indicator */}
          <div className="flex items-center space-x-4">
            {/* API Status Indicator */}
            <div className="hidden lg:flex items-center space-x-2 px-3 py-1.5 bg-green-500/20 border border-green-400/30 rounded-full">
              <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
              <span className="text-xs font-medium text-green-100">API Online</span>
            </div>
          </div>
        </div>
      </div>
      
      {/* Subtle bottom border with gradient */}
      <div className="h-px bg-gradient-to-r from-transparent via-blue-400/30 to-transparent"></div>
    </nav>
  )
}

export default Navigation