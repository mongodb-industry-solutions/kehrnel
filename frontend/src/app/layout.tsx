import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import Navigation from '@/components/Navigation'
import ErrorBoundary from '@/components/ErrorBoundary'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'KEHRNEL - OpenEHR API Documentation',
  description: 'Comprehensive documentation for OpenEHR API implementation with Python, FastAPI, and MongoDB',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} antialiased bg-gray-50`}>
        <ErrorBoundary>
          <Navigation />
          <main className="min-h-screen">
            {children}
          </main>
        </ErrorBoundary>
      </body>
    </html>
  )
}
