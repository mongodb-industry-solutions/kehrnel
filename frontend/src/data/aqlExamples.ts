export interface AQLExample {
  id: number
  title: string
  difficulty: 'basic' | 'intermediate' | 'complex'
  description: string
  concepts: string[]
  query: string
  explanation: string
  expectedResult?: string
  tips?: string[]
}

export const aqlExamples: AQLExample[] = [
  {
    id: 1,
    title: "Select the UID of All Vaccination List Compositions",
    difficulty: "basic",
    description: "This is the most fundamental query. It retrieves the unique ID (uid) for every composition that matches the specified archetype ID.",
    concepts: ["SELECT", "FROM", "CONTAINS", "Composition UID"],
    query: `SELECT
  c/uid/value
FROM
  EHR e
  CONTAINS COMPOSITION c [openEHR-EHR-COMPOSITION.vaccination_list.v0]`,
    explanation: `This query introduces the basic AQL structure:
    
• **SELECT**: Specifies what data to retrieve (composition UID)
• **FROM**: Defines the source (EHR contains COMPOSITION)
• **CONTAINS**: OpenEHR's hierarchical relationship operator
• **Archetype ID**: Specifies the exact composition type we want

The result will be a list of unique identifiers for all vaccination compositions in the system.`,
    expectedResult: "List of composition UIDs (e.g., '01234567-89ab-cdef-0123-456789abcdef::local.ehrbase.org::1')",
    tips: [
      "Every OpenEHR composition has a unique UID",
      "Archetype IDs follow the format: openEHR-EHR-COMPOSITION.name.version",
      "The CONTAINS keyword is fundamental to AQL - it represents OpenEHR's hierarchical structure"
    ]
  },
  {
    id: 2,
    title: "Select Composition Name and Context Start Time with Aliases",
    difficulty: "basic",
    description: "This query retrieves high-level data from the composition and uses the AS keyword to assign user-friendly names to the result columns.",
    concepts: ["SELECT multiple fields", "AS aliases", "Context data", "Composition metadata"],
    query: `SELECT
  c/uid/value,
  c/name/value AS composition_name,
  c/context/start_time/value AS recorded_date
FROM
  EHR e
  CONTAINS COMPOSITION c [openEHR-EHR-COMPOSITION.vaccination_list.v0]`,
    explanation: `This query builds on the previous one by:

• **Multiple SELECT fields**: Retrieving several pieces of data at once
• **AS aliases**: Using friendly names for result columns
• **Composition metadata**: Accessing built-in composition properties
• **Context data**: Retrieving contextual information like timing

The 'context/start_time' represents when the clinical session or event began.`,
    expectedResult: "Table with three columns: uid/value, composition_name, recorded_date",
    tips: [
      "Always use aliases (AS) to make your results more readable",
      "Context data includes timing, location, and other session information",
      "The 'name/value' path gets the human-readable composition name"
    ]
  },
  {
    id: 3,
    title: "Retrieve the Time of Each Immunization Action",
    difficulty: "intermediate",
    description: "This query uses a CONTAINS clause to navigate into the ACTION archetype and select the administration time of the medication.",
    concepts: ["Nested CONTAINS", "ACTION archetype", "Clinical data", "Time values"],
    query: `SELECT
  med_ac/time/value AS administration_time
FROM
  EHR e
  CONTAINS COMPOSITION c
  CONTAINS ACTION med_ac [openEHR-EHR-ACTION.medication.v1]`,
    explanation: `This query introduces nested containment:

• **Nested CONTAINS**: EHR contains COMPOSITION which contains ACTION
• **ACTION archetype**: Represents clinical actions (like giving medication)
• **Generic COMPOSITION**: No specific archetype - matches any composition type
• **Clinical timing**: The 'time/value' represents when the action occurred

This pattern is fundamental to accessing clinical data within compositions.`,
    expectedResult: "List of timestamps when medications were administered",
    tips: [
      "ACTION archetypes represent things that were done (medications, procedures, etc.)",
      "You can omit the archetype ID to match any composition type",
      "Time values are usually in ISO 8601 format"
    ]
  },
  {
    id: 4,
    title: "Filter Immunizations Administered Before a Specific Date",
    difficulty: "intermediate",
    description: "This query introduces the WHERE clause to filter the results, finding only immunizations that occurred before the year 2010.",
    concepts: ["WHERE clause", "Date filtering", "Comparison operators", "Data filtering"],
    query: `SELECT
  med_ac/time/value
FROM
  EHR e
  CONTAINS COMPOSITION c
  CONTAINS ACTION med_ac [openEHR-EHR-ACTION.medication.v1]
WHERE
  med_ac/time/value < '2010-01-01T00:00:00Z'`,
    explanation: `This query introduces conditional filtering:

• **WHERE clause**: Filters results based on conditions
• **Date comparison**: Using less-than operator with ISO 8601 date
• **UTC timezone**: The 'Z' suffix indicates UTC time
• **Boolean logic**: Only records meeting the condition are returned

This is essential for analyzing historical data or finding specific time periods.`,
    expectedResult: "Only administration times from before January 1, 2010",
    tips: [
      "Always use ISO 8601 format for dates: YYYY-MM-DDTHH:MM:SSZ",
      "The 'Z' suffix means UTC timezone",
      "You can use >, <, =, >=, <= operators for date comparisons"
    ]
  },
  {
    id: 5,
    title: "Select the Specific Immunization Item Code",
    difficulty: "intermediate",
    description: "This query demonstrates how to navigate complex paths that include at-codes to retrieve a specific coded text value from within the ACTION/description.",
    concepts: ["Complex paths", "at-codes", "Coded values", "Archetype navigation"],
    query: `SELECT
  med_ac/description[at0017]/items[at0020]/value/defining_code/code_string AS immunization_code
FROM
  EHR e
  CONTAINS COMPOSITION c
  CONTAINS ACTION med_ac [openEHR-EHR-ACTION.medication.v1]`,
    explanation: `This query shows advanced path navigation:

• **at-codes**: Archetype constraint codes (like at0017, at0020)
• **Deep paths**: Navigating through multiple levels of structure
• **Coded values**: Accessing standardized medical codes
• **Complex structure**: description → items → value → defining_code

at-codes represent specific constraints within archetypes, ensuring data consistency.`,
    expectedResult: "Medical codes for specific immunization types (e.g., 'MCC', 'BCG', 'DTP')",
    tips: [
      "at-codes are defined in the archetype and ensure data consistency",
      "coded values often contain both code_string and terminology_id",
      "Use archetype documentation to understand the structure"
    ]
  },
  {
    id: 6,
    title: "Retrieve Data from the other_context (Admin Cluster)",
    difficulty: "intermediate",
    description: "This query shows how to access data stored in the composition's context, specifically targeting the admin_salut cluster to get the publishing center code.",
    concepts: ["Context clusters", "Administrative data", "CLUSTER archetype", "Publishing information"],
    query: `SELECT
  admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string AS publishing_center
FROM
  EHR e
  CONTAINS COMPOSITION c
  CONTAINS CLUSTER admin_salut [openEHR-EHR-CLUSTER.admin_salut.v0]`,
    explanation: `This query accesses administrative context data:

• **CLUSTER archetype**: Reusable data groups within compositions
• **Administrative context**: Non-clinical metadata about the record
• **Nested structure**: items within items, showing hierarchical organization
• **Publishing center**: Identifies which healthcare center created the record

Context clusters contain metadata about how, when, and where data was recorded.`,
    expectedResult: "Healthcare center codes (e.g., 'E08665478', 'HOSP001')",
    tips: [
      "CLUSTER archetypes are reusable components within compositions",
      "Context data includes administrative, not clinical information",
      "Publishing center helps with data provenance and auditing"
    ]
  },
  {
    id: 7,
    title: "Find EHRs with a Specific Vaccine Code and Performer ID",
    difficulty: "complex",
    description: "This query combines two WHERE conditions with AND to find EHRs that received a specific vaccine (code 'MCC') administered by a specific performer (ID '01817273').",
    concepts: ["Multiple WHERE conditions", "AND logic", "Performer data", "Complex filtering"],
    query: `SELECT
  e/ehr_id/value
FROM
  EHR e
  CONTAINS COMPOSITION c
  CONTAINS ACTION med_ac [openEHR-EHR-ACTION.medication.v1]
WHERE
  med_ac/description[at0017]/items[at0020]/value/defining_code/code_string = 'MCC'
  AND med_ac/other_participations/performer/identifiers/id = '01817273'`,
    explanation: `This query demonstrates complex filtering:

• **Multiple conditions**: Using AND to combine filter criteria
• **Exact matching**: Using = operator for precise matches
• **Performer tracking**: Identifying who administered the medication
• **EHR identification**: Returning the patient's EHR ID

This is useful for audit trails, quality control, and provider-specific analysis.`,
    expectedResult: "EHR IDs of patients who received MCC vaccine from performer 01817273",
    tips: [
      "Use AND/OR to combine multiple filter conditions",
      "Performer data tracks healthcare provider information",
      "This pattern is essential for audit trails and compliance"
    ]
  },
  {
    id: 8,
    title: "Select Nested Medication Details and Order by Time",
    difficulty: "complex",
    description: "This query navigates through multiple nested archetypes (ACTION contains a CLUSTER) to retrieve the detailed medication name code and orders the results by the administration time in descending order.",
    concepts: ["Nested archetypes", "ORDER BY", "Deep nesting", "Result sorting"],
    query: `SELECT
  med_ac/time/value AS administration_time,
  medication_details/items[at0132]/value/defining_code/code_string AS medication_code
FROM
  EHR e
  CONTAINS COMPOSITION c
  CONTAINS ACTION med_ac [openEHR-EHR-ACTION.medication.v1]
    CONTAINS CLUSTER medication_details [openEHR-EHR-CLUSTER.medication.v2]
ORDER BY
  med_ac/time/value DESC`,
    explanation: `This query shows advanced archetype nesting and sorting:

• **Nested CONTAINS**: ACTION containing CLUSTER showing multi-level nesting
• **Deep data access**: Reaching medication details within the action
• **ORDER BY**: Sorting results by administration time
• **DESC**: Descending order (newest first)

This pattern is common when accessing detailed clinical information.`,
    expectedResult: "Medications with times, sorted from most recent to oldest",
    tips: [
      "Use ORDER BY to sort results chronologically or alphabetically",
      "DESC = descending (newest first), ASC = ascending (oldest first)",
      "Nested CONTAINS allows access to detailed structured data"
    ]
  },
  {
    id: 9,
    title: "Find Compositions Based on Context and Content Data Simultaneously",
    difficulty: "complex",
    description: "This query retrieves compositions that meet criteria in both the administrative context (admin_salut cluster) and the clinical content (ACTION archetype).",
    concepts: ["Multiple archetype types", "Context AND content", "Complex boolean logic", "Multi-criteria filtering"],
    query: `SELECT
  e/ehr_id/value,
  c/context/start_time/value AS composition_date
FROM
  EHR e
  CONTAINS COMPOSITION c [openEHR-EHR-COMPOSITION.vaccination_list.v0]
    CONTAINS (
      CLUSTER admin_salut [openEHR-EHR-CLUSTER.admin_salut.v0] AND 
      ACTION med_ac [openEHR-EHR-ACTION.medication.v1]
    )
WHERE
  admin_salut/items[at0010]/items[at0017]/value/defining_code/code_string = 'E08665478'
  AND med_ac/ism_transition/current_state/defining_code/code_string = '245'`,
    explanation: `This advanced query combines multiple archetype types:

• **Parenthetical CONTAINS**: Grouping multiple archetype requirements
• **Cross-archetype filtering**: Conditions on both admin and clinical data
• **ISM transition**: Accessing the state machine of the action
• **Multi-level validation**: Ensuring both context and content match criteria

This pattern is powerful for complex clinical data analysis.`,
    expectedResult: "EHR IDs and dates for compositions meeting both administrative and clinical criteria",
    tips: [
      "Use parentheses to group multiple CONTAINS requirements",
      "ISM transitions track the state of clinical actions",
      "This pattern ensures data quality across multiple dimensions"
    ]
  },
  {
    id: 10,
    title: "Complex Query: Full Correlation with Multiple Selections and Filters",
    difficulty: "complex",
    description: "This comprehensive query links information across the entire composition. It finds vaccination compositions from a specific publishing center where a specific vaccine was administered after 2005, selects data from both context and action, and orders the results.",
    concepts: ["Full composition traversal", "Multiple SELECT fields", "Complex WHERE", "ORDER BY", "Complete data correlation"],
    query: `SELECT
  c/uid/value AS composition_uid,
  admin_salut/items[at0001]/value/value AS authorization_date,
  med_ac/time/value AS administration_time,
  med_ac/description[at0017]/items[at0020]/value/value AS vaccine_name
FROM
  EHR e
  CONTAINS COMPOSITION c [openEHR-EHR-COMPOSITION.vaccination_list.v0]
    CONTAINS (
      CLUSTER admin_salut [openEHR-EHR-CLUSTER.admin_salut.v0] AND 
      SECTION [openEHR-EHR-SECTION.immunisation_list.v0]
        CONTAINS ACTION med_ac [openEHR-EHR-ACTION.medication.v1]
    )
WHERE
  admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string = 'E08665478'
  AND med_ac/time/value > '2005-01-01T00:00:00Z'
  AND med_ac/description[at0017]/items[at0020]/value/defining_code/code_string = 'MCC'
ORDER BY
  med_ac/time/value ASC`,
    explanation: `This is a comprehensive query showcasing all major AQL features:

• **Multiple SELECT fields**: Composition, administrative, and clinical data
• **Complex nesting**: EHR → COMPOSITION → (CLUSTER AND SECTION → ACTION)
• **Multi-condition WHERE**: Publishing center, date range, and vaccine type
• **Cross-archetype correlation**: Linking administrative and clinical information
• **Ordered results**: Chronological ordering for analysis

This represents a real-world clinical data query pattern.`,
    expectedResult: "Complete vaccination records with UIDs, dates, times, and vaccine names, chronologically ordered",
    tips: [
      "This pattern is typical for comprehensive clinical reports",
      "SECTION archetypes organize clinical content within compositions",
      "Use ASC ordering for chronological analysis of patient care"
    ]
  }
]

export const difficultyColors = {
  basic: {
    bg: 'bg-green-100',
    text: 'text-green-800',
    border: 'border-green-200',
    icon: 'text-green-500'
  },
  intermediate: {
    bg: 'bg-yellow-100',
    text: 'text-yellow-800',
    border: 'border-yellow-200',
    icon: 'text-yellow-500'
  },
  complex: {
    bg: 'bg-red-100',
    text: 'text-red-800',
    border: 'border-red-200',
    icon: 'text-red-500'
  }
} as const

export const difficultyLabels = {
  basic: 'Basic',
  intermediate: 'Intermediate',
  complex: 'Complex'
} as const