import { Metadata } from 'next'
import AQLPlaygroundComponent from '@/components/AQLPlayground'

export const metadata: Metadata = {
  title: 'AQL Playground - Learn OpenEHR Query Language - KEHRNEL',
  description: 'Interactive AQL (Archetype Query Language) playground. Learn OpenEHR queries from basic to advanced with real examples and live testing.',
  keywords: ['AQL', 'OpenEHR', 'Archetype Query Language', 'EHR queries', 'healthcare data'],
}

export default function AQLPlaygroundPage() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
      <AQLPlaygroundComponent />
    </div>
  )
}