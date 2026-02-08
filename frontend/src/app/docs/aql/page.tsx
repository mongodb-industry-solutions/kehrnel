import { Metadata } from 'next'
import CodeBlock from '@/components/CodeBlock'
import Link from 'next/link'

export const metadata: Metadata = {
  title: 'AQL (Archetype Query Language) - KEHRNEL OpenEHR API',
  description: 'Complete documentation for the AQL API including SELECT statements, transformations, and supported operations.',
}

export default function AQLDocsPage() {
  return (
    <div className="min-h-screen bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="max-w-4xl mx-auto">
          {/* Header */}
          <div className="mb-8">
            <nav className="text-sm breadcrumbs mb-4">
              <ol className="flex space-x-2 text-gray-500">
                <li><Link href="/" className="hover:text-blue-600">Home</Link></li>
                <li>/</li>
                <li><Link href="/docs" className="hover:text-blue-600">Docs</Link></li>
                <li>/</li>
                <li className="text-gray-900">AQL</li>
              </ol>
            </nav>
            <h1 className="text-4xl font-bold text-gray-900 mb-4">
              AQL (Archetype Query Language)
            </h1>
            <p className="text-xl text-gray-600">
              The Archetype Query Language (AQL) is a declarative query language for expressing queries 
              to retrieve data from OpenEHR repositories.
            </p>
          </div>

          {/* Table of Contents */}
          <div className="bg-gray-50 rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Table of Contents</h2>
            <ul className="space-y-2 text-blue-600">
              <li><a href="#select-statements" className="hover:text-blue-800">SELECT Statements</a></li>
              <li><a href="#from-clause" className="hover:text-blue-800">FROM Clause</a></li>
              <li><a href="#where-clause" className="hover:text-blue-800">WHERE Clause</a></li>
              <li><a href="#contains-clause" className="hover:text-blue-800">CONTAINS Clause</a></li>
              <li><a href="#supported-operations" className="hover:text-blue-800">Supported Operations</a></li>
              <li><a href="#examples" className="hover:text-blue-800">Examples</a></li>
              <li><a href="#limitations" className="hover:text-blue-800">Current Limitations</a></li>
            </ul>
          </div>

          {/* SELECT Statements */}
          <section id="select-statements" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">SELECT Statements</h2>
            <p className="text-gray-700 mb-4">
              The SELECT statement allows retrieving single values or objects defined in the CONTAINS clause. 
              The syntax always starts with the keyword SELECT, optionally followed by DISTINCT, and then one 
              or more column expressions.
            </p>
            
            <p className="text-gray-700 mb-4">
              A column expression is formed by an identified path, a function, a literal value, or a plain 
              variable name defined in the FROM clause. Where a variable name is specified, the full object 
              of the type associated with the variable is retrieved, such as a COMPOSITION, OBSERVATION, etc.
            </p>

            <p className="text-gray-700 mb-6">
              Each column expression may have a name alias renaming the associated data. When the SELECT clause 
              contains multiple column expressions, they are separated using a comma. Note that the alias cannot 
              be used within the WHERE clause, as the WHERE clause is processed before the SELECT clause, in 
              accordance with the SQL standard.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-6">
              <h3 className="text-lg font-semibold mb-3">Basic SELECT Syntax</h3>
              <CodeBlock
                language="sql"
                code={`SELECT column_expression [AS alias] [, column_expression [AS alias] ...]
FROM ehr_class [identifier] [, ehr_class [identifier] ...]
[WHERE where_expression]
[ORDER BY order_expression]
[LIMIT count [OFFSET start]]`}
              />
            </div>

            <div className="mb-6">
              <h3 className="text-lg font-semibold mb-3">Example: Basic SELECT</h3>
              <CodeBlock
                language="sql"
                code={`SELECT c/uid/value, c/context/start_time
FROM COMPOSITION c
WHERE c/name/value = 'Vital Signs'`}
              />
            </div>
          </section>

          {/* FROM Clause */}
          <section id="from-clause" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">FROM Clause</h2>
            <p className="text-gray-700 mb-4">
              The FROM clause specifies the EHR class from which data is to be retrieved. Currently supported classes include:
            </p>
            
            <ul className="list-disc list-inside text-gray-700 mb-6 space-y-2">
              <li><strong>EHR</strong> - Electronic Health Record</li>
              <li><strong>COMPOSITION</strong> - Clinical documents</li>
              <li><strong>OBSERVATION</strong> - Clinical observations</li>
              <li><strong>EVALUATION</strong> - Clinical evaluations</li>
              <li><strong>INSTRUCTION</strong> - Clinical instructions</li>
              <li><strong>ACTION</strong> - Clinical actions</li>
            </ul>

            <div className="mb-6">
              <h3 className="text-lg font-semibold mb-3">Example: FROM with identifier</h3>
              <CodeBlock
                language="sql"
                code={`SELECT e/ehr_id/value, c/uid/value
FROM EHR e 
CONTAINS COMPOSITION c
WHERE c/archetype_details/archetype_id/value = 'openEHR-EHR-COMPOSITION.encounter.v1'`}
              />
            </div>
          </section>

          {/* WHERE Clause */}
          <section id="where-clause" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">WHERE Clause</h2>
            <p className="text-gray-700 mb-4">
              The WHERE clause filters the results based on specified conditions. Our implementation supports:
            </p>
            
            <ul className="list-disc list-inside text-gray-700 mb-6 space-y-2">
              <li>Comparison operators: =, !=, &lt;, &lt;=, &gt;, &gt;=</li>
              <li>Logical operators: AND, OR, NOT</li>
              <li>String matching: LIKE, MATCHES (regex)</li>
              <li>Null checks: IS NULL, IS NOT NULL</li>
              <li>Set operations: IN, NOT IN</li>
            </ul>

            <div className="mb-6">
              <h3 className="text-lg font-semibold mb-3">Example: Complex WHERE conditions</h3>
              <CodeBlock
                language="sql"
                code={`SELECT c/uid/value, o/data[at0001]/items[at0004]/value/magnitude
FROM COMPOSITION c
CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
WHERE c/context/start_time >= '2023-01-01T00:00:00'
  AND c/context/start_time <= '2023-12-31T23:59:59'
  AND o/data[at0001]/items[at0004]/value/magnitude > 140`}
              />
            </div>
          </section>

          {/* CONTAINS Clause */}
          <section id="contains-clause" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">CONTAINS Clause</h2>
            <p className="text-gray-700 mb-4">
              The CONTAINS clause specifies the hierarchical relationships between OpenEHR objects. 
              It defines how one archetype contains another within the information model.
            </p>

            <div className="mb-6">
              <h3 className="text-lg font-semibold mb-3">Example: Nested CONTAINS</h3>
              <CodeBlock
                language="sql"
                code={`SELECT c/uid/value, o/data[at0001]/items[at0004]/value/magnitude AS systolic
FROM EHR e
CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
WHERE e/ehr_id/value = $ehr_id`}
              />
            </div>
          </section>

          {/* Supported Operations */}
          <section id="supported-operations" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">Supported Operations</h2>
            <p className="text-gray-700 mb-4">
              Our AQL implementation currently supports the following operations and transformations:
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
              <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-green-800 mb-3">✓ Supported Features</h3>
                <ul className="list-disc list-inside text-green-700 space-y-1 text-sm">
                  <li>Basic SELECT statements</li>
                  <li>FROM clause with EHR classes</li>
                  <li>Simple WHERE conditions</li>
                  <li>Path expressions</li>
                  <li>Basic CONTAINS relationships</li>
                  <li>Comparison operators</li>
                  <li>String literals</li>
                  <li>Numeric values</li>
                </ul>
              </div>

              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-yellow-800 mb-3">⚠ Partial Support</h3>
                <ul className="list-disc list-inside text-yellow-700 space-y-1 text-sm">
                  <li>Complex nested CONTAINS</li>
                  <li>Advanced path expressions</li>
                  <li>Some aggregate functions</li>
                  <li>DISTINCT clause</li>
                  <li>ORDER BY clause</li>
                  <li>LIMIT and OFFSET</li>
                </ul>
              </div>
            </div>
          </section>

          {/* Examples */}
          <section id="examples" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">Examples</h2>
            
            <div className="space-y-8">
              <div>
                <h3 className="text-lg font-semibold mb-3">1. Get all compositions for a specific EHR</h3>
                <CodeBlock
                  language="sql"
                  code={`SELECT c/uid/value, c/name/value, c/context/start_time
FROM EHR e
CONTAINS COMPOSITION c
WHERE e/ehr_id/value = $ehr_id`}
                />
              </div>

              <div>
                <h3 className="text-lg font-semibold mb-3">2. Find blood pressure observations</h3>
                <CodeBlock
                  language="sql"
                  code={`SELECT 
    c/uid/value AS composition_id,
    o/data[at0001]/items[at0004]/value/magnitude AS systolic,
    o/data[at0001]/items[at0005]/value/magnitude AS diastolic
FROM COMPOSITION c
CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
WHERE c/context/start_time >= '2023-01-01T00:00:00'`}
                />
              </div>

              <div>
                <h3 className="text-lg font-semibold mb-3">3. Complex query with multiple conditions</h3>
                <CodeBlock
                  language="sql"
                  code={`SELECT 
    e/ehr_id/value AS patient_id,
    c/name/value AS document_name,
    c/context/start_time AS created_time
FROM EHR e
CONTAINS COMPOSITION c
WHERE c/archetype_details/archetype_id/value LIKE 'openEHR-EHR-COMPOSITION%'
  AND c/context/start_time >= '2023-01-01T00:00:00'
  AND c/context/start_time <= '2023-12-31T23:59:59'`}
                />
              </div>
            </div>
          </section>

          {/* Limitations */}
          <section id="limitations" className="mb-12">
            <h2 className="text-3xl font-bold text-gray-900 mb-6">Current Limitations</h2>
            
            <div className="bg-red-50 border border-red-200 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-red-800 mb-4">Known Limitations</h3>
              <ul className="list-disc list-inside text-red-700 space-y-2">
                <li>Complex nested CONTAINS with multiple levels may not be fully supported</li>
                <li>Some advanced path expressions and functions are not implemented</li>
                <li>Advanced aggregation functions (COUNT, SUM, AVG) have limited support</li>
                <li>Subqueries are not currently supported</li>
                <li>Complex JOIN operations between different EHR classes</li>
                <li>Some OpenEHR-specific functions and operators</li>
              </ul>
            </div>

            <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-blue-800 mb-4">Development Roadmap</h3>
              <p className="text-blue-700 mb-4">
                We are continuously improving our AQL implementation. Future updates will include:
              </p>
              <ul className="list-disc list-inside text-blue-700 space-y-2">
                <li>Enhanced support for complex CONTAINS relationships</li>
                <li>Full implementation of aggregate functions</li>
                <li>Advanced path expression support</li>
                <li>Subquery capabilities</li>
                <li>Performance optimizations for large datasets</li>
              </ul>
            </div>
          </section>

          {/* Navigation */}
          <div className="flex justify-between items-center pt-8 border-t border-gray-200">
            <Link
              href="/docs"
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              ← Back to Documentation
            </Link>
            <Link
              href="/docs/composition"
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              Composition API →
            </Link>
          </div>
        </div>
      </div>
    </div>
  )
}