import { Metadata } from 'next'
import APIExplorerWithSidebar from '@/components/APIExplorerWithSidebar'
import ErrorBoundary, { APIErrorFallback } from '@/components/ErrorBoundary'

export const metadata: Metadata = {
  title: 'OpenEHR API Explorer & Testing - KEHRNEL',
  description: 'Interactive API documentation and live testing interface for the OpenEHR API endpoints.',
}

export default function APIPage() {
  return (
    <div className="h-screen bg-gray-50">
      <ErrorBoundary fallback={APIErrorFallback}>
        <APIExplorerWithSidebar />
      </ErrorBoundary>
    </div>
  )
}