import Link from 'next/link'
import { BookOpenIcon, CodeBracketIcon, ServerIcon, BeakerIcon } from '@heroicons/react/24/outline'

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      {/* Hero Section */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-20 pb-16">
        <div className="text-center">
          <h1 className="text-4xl md:text-6xl font-bold text-gray-900 mb-6">
            OpenEHR API
            <span className="text-blue-600"> Documentation</span>
          </h1>
          <p className="text-xl md:text-2xl text-gray-600 mb-8 max-w-3xl mx-auto">
            Comprehensive documentation for our OpenEHR API implementation built with Python, FastAPI, and MongoDB
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/docs/aql"
              className="inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors"
            >
              Get Started
            </Link>
            <Link
              href="/api"
              className="inline-flex items-center px-6 py-3 border border-gray-300 text-base font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 transition-colors"
            >
              Explore API
            </Link>
          </div>
        </div>
      </div>

      {/* Features Section */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
        <h2 className="text-3xl font-bold text-center text-gray-900 mb-12">
          API Resources & Features
        </h2>
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
          {/* Documentation */}
          <div className="bg-white rounded-lg shadow-lg p-6 hover:shadow-xl transition-shadow">
            <div className="flex items-center justify-center w-12 h-12 bg-blue-100 rounded-lg mb-4">
              <BookOpenIcon className="h-6 w-6 text-blue-600" />
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">Documentation</h3>
            <p className="text-gray-600 mb-4">
              Detailed guides for AQL, Compositions, Contributions, EHR, and EHR Status resources.
            </p>
            <Link
              href="/docs/aql"
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              Read Docs →
            </Link>
          </div>

          {/* API Explorer */}
          <div className="bg-white rounded-lg shadow-lg p-6 hover:shadow-xl transition-shadow">
            <div className="flex items-center justify-center w-12 h-12 bg-green-100 rounded-lg mb-4">
              <CodeBracketIcon className="h-6 w-6 text-green-600" />
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">API Explorer</h3>
            <p className="text-gray-600 mb-4">
              Interactive API documentation with live examples and testing capabilities.
            </p>
            <Link
              href="/api"
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              Explore API →
            </Link>
          </div>

          {/* OpenEHR Compliance */}
          <div className="bg-white rounded-lg shadow-lg p-6 hover:shadow-xl transition-shadow">
            <div className="flex items-center justify-center w-12 h-12 bg-purple-100 rounded-lg mb-4">
              <ServerIcon className="h-6 w-6 text-purple-600" />
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">OpenEHR Compliant</h3>
            <p className="text-gray-600 mb-4">
              Built following OpenEHR specifications with comprehensive resource coverage.
            </p>
            <Link
              href="https://specifications.openehr.org/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              OpenEHR Specs →
            </Link>
          </div>

          {/* Data Lab */}
          <div className="bg-white rounded-lg shadow-lg p-6 hover:shadow-xl transition-shadow">
            <div className="flex items-center justify-center w-12 h-12 bg-orange-100 rounded-lg mb-4">
              <BeakerIcon className="h-6 w-6 text-orange-600" />
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">Data Lab</h3>
            <p className="text-gray-600 mb-4">
              Advanced analytics and data exploration platform for OpenEHR data.
            </p>
            <Link
              href="#"
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              Visit Data Lab →
            </Link>
          </div>
        </div>
      </div>

      {/* Quick Start Section */}
      <div className="bg-white py-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center">
            <h2 className="text-3xl font-bold text-gray-900 mb-8">
              Quick Start
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
              <div className="text-center">
                <div className="bg-blue-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4">
                  <span className="text-2xl font-bold text-blue-600">1</span>
                </div>
                <h3 className="text-xl font-semibold mb-2">Explore Resources</h3>
                <p className="text-gray-600">
                  Learn about our OpenEHR API resources and their capabilities
                </p>
              </div>
              <div className="text-center">
                <div className="bg-blue-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4">
                  <span className="text-2xl font-bold text-blue-600">2</span>
                </div>
                <h3 className="text-xl font-semibold mb-2">Test the API</h3>
                <p className="text-gray-600">
                  Use our interactive API explorer to test endpoints and operations
                </p>
              </div>
              <div className="text-center">
                <div className="bg-blue-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4">
                  <span className="text-2xl font-bold text-blue-600">3</span>
                </div>
                <h3 className="text-xl font-semibold mb-2">Build Applications</h3>
                <p className="text-gray-600">
                  Start building healthcare applications with our OpenEHR API
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
