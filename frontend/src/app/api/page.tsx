import { Metadata } from 'next'
import APIExplorer from '@/components/APIExplorer'
import ErrorBoundary, { APIErrorFallback } from '@/components/ErrorBoundary'

export const metadata: Metadata = {
  title: 'OpenEHR API Explorer - KEHRNEL',
  description: 'Interactive API documentation and testing interface for the OpenEHR API endpoints.',
}

export default function APIPage() {
  return (
    <div className="min-h-screen bg-gray-50">
      <ErrorBoundary fallback={APIErrorFallback}>
        <APIExplorer />
      </ErrorBoundary>
    </div>
  )
}