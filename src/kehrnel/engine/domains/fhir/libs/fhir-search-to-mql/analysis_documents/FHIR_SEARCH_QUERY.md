# FHIR Search Query Language - Comprehensive Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Search Contexts](#search-contexts)
3. [Search Parameter Types](#search-parameter-types)
4. [Search Modifiers](#search-modifiers)
5. [Search Prefixes](#search-prefixes)
6. [Special Search Conditions](#special-search-conditions)
7. [Modifying Search Results](#modifying-search-results)
8. [Standard Parameters](#standard-parameters)
9. [Composite Parameters](#composite-parameters)
10. [Advanced Examples](#advanced-examples)

---

## Introduction

FHIR Search is the primary mechanism for finding and listing resource instances. The search mechanism is designed to be flexible enough to meet the needs of a wide variety of use cases while being simple enough to be commonly useful.

### Basic Search Syntax

```
GET [base]/[type]?[parameter]=[value]
```

**Example:**
```
GET [base]/Patient?name=John
GET [base]/Observation?code=12345-6&date=gt2020-01-01
```

### Search Response

FHIR Search returns a `Bundle` resource with a type of `searchset`, with matching resources appearing as individual entries.

---

## Search Contexts

FHIR searches can be executed in three defined contexts:

### 1. All Resource Types
```http
GET [base]?[parameter]=[value]
GET [base]?_type=Patient,Observation&identifier=12345
```

### 2. Specified Resource Type
```http
GET [base]/Patient?name=Smith
GET [base]/Observation?code=8480-6
```

### 3. Specified Compartment
```http
GET [base]/Patient/123/Observation?code=8480-6
GET [base]/Encounter/456/Condition
```

---

## Search Parameter Types

FHIR defines several search parameter types, each with specific behavior:

### 1. **Number**

Searches on numerical values with precision awareness.

**Syntax:**
```
[parameter]=[number]
[parameter]=[prefix][number]
```

**Examples:**
```http
# Exact match with 3 significant figures (99.5 to 100.5)
GET [base]/RiskAssessment?probability=100

# Exact match with 5 significant figures (99.995 to 100.005)
GET [base]/RiskAssessment?probability=100.00

# Exponential notation (1 significant figure: 50 to 150)
GET [base]/RiskAssessment?probability=1e2

# Greater than exactly 100
GET [base]/RiskAssessment?probability=gt100

# Less than or equal to 100
GET [base]/RiskAssessment?probability=le100

# Not equal to 100
GET [base]/RiskAssessment?probability=ne100

# Range search
GET [base]/ImmunizationRecommendation?dose-number=2
```

---

### 2. **Date**

Searches on date/time values or periods with various precision levels.

**Format:** `yyyy-mm-ddThh:mm:ss[Z|(+|-)hh:mm]`

**Examples:**
```http
# Exact date match (entire day)
GET [base]/Patient?birthdate=1970-01-01

# Date range - equal to
GET [base]/Observation?date=eq2013-01-14

# Date range - not equal
GET [base]/Observation?date=ne2013-01-14

# Less than
GET [base]/Observation?date=lt2013-01-14T10:00

# Greater than
GET [base]/Observation?date=gt2013-01-14T10:00

# Greater than or equal
GET [base]/AllergyIntolerance?onset-date=ge2013-03-14

# Less than or equal
GET [base]/AllergyIntolerance?onset-date=le2013-03-14

# Starts after (non-inclusive)
GET [base]/Encounter?date=sa2013-03-14

# Ends before (non-inclusive)
GET [base]/Encounter?date=eb2013-03-14

# Approximately (within ~10%)
GET [base]/Observation?date=ap2013-03-14

# Date with time
GET [base]/Observation?date=2020-01-15T14:30:00Z

# Period searches
GET [base]/Encounter?period=ge2020-01-01&period=le2020-12-31

# Year precision
GET [base]/Patient?birthdate=1980

# Month precision
GET [base]/Patient?birthdate=1980-06
```

---

### 3. **String**

Case-insensitive and accent-insensitive string matching.

**Default Behavior:** Matches if the value equals or starts with the supplied parameter.

**Examples:**
```http
# Basic string search (starts with, case-insensitive)
GET [base]/Patient?given=eve

# Will match: "Eve", "Evelyn", "Everett"
# Won't match: "Steve" (doesn't start with "eve")

# Family name search
GET [base]/Patient?family=smith

# Address search
GET [base]/Patient?address=123 Main St

# City search
GET [base]/Patient?address-city=Boston

# Postal code search
GET [base]/Patient?address-postalcode=02134

# Name search (any part of HumanName)
GET [base]/Patient?name=John Smith

# Multiple parameters (AND)
GET [base]/Patient?given=John&family=Smith

# Multiple values (OR)
GET [base]/Patient?given=John,Jane

# Contains modifier (matches anywhere in string)
GET [base]/Patient?family:contains=son
# Will match: "Son", "Sonder", "Erikson", "Samsonite"

# Exact modifier (exact match including case)
GET [base]/Patient?family:exact=Smith
# Will match only: "Smith" (not "smith" or "SMITH")
```

---

### 4. **Token**

Searches on coded elements with optional system namespace.

**Syntax:**
```
[parameter]=[code]
[parameter]=[system]|[code]
[parameter]=|[code]
[parameter]=[system]|
```

**Examples:**
```http
# Code only (any system)
GET [base]/Patient?gender=male

# System and code
GET [base]/Condition?code=http://snomed.info/sct|73211009

# Code with empty system
GET [base]/Observation?code=|12345

# System only (any code from that system)
GET [base]/Observation?code=http://loinc.org|

# Not modifier
GET [base]/Patient?gender:not=male

# Boolean values
GET [base]/Patient?active=true
GET [base]/Patient?active=false

# Identifier searches
GET [base]/Patient?identifier=12345
GET [base]/Patient?identifier=http://acme.org/mrn|12345

# Identifier of-type modifier
GET [base]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|446053

# Code system hierarchy - in modifier
GET [base]/Condition?code:in=http://snomed.info/sct?fhir_vs=isa/126851005

# Code system hierarchy - below modifier
GET [base]/Condition?code:below=126851005

# Code system hierarchy - above modifier
GET [base]/Condition?code:above=126851005

# Text search on code display
GET [base]/Condition?code:text=headache

# Contact point search
GET [base]/Patient?telecom=555-1234
GET [base]/Patient?email=john@example.com
GET [base]/Patient?phone=555-1234
```

---

### 5. **Reference**

Searches on references between resources.

**Syntax:**
```
[parameter]=[id]
[parameter]=[type]/[id]
[parameter]=[url]
```

**Examples:**
```http
# Logical ID reference
GET [base]/Observation?subject=123

# Type/ID reference
GET [base]/Observation?subject=Patient/123

# Type modifier
GET [base]/Observation?subject:Patient=123

# Full URL reference
GET [base]/Observation?subject=https://example.org/fhir/Patient/123

# Versioned reference
GET [base]/Observation?subject=Patient/123/_history/5

# Identifier search (instead of reference)
GET [base]/Observation?subject:identifier=http://example.org/mrn|12345

# Multiple references (OR)
GET [base]/Observation?subject=Patient/123,Patient/456

# Chaining - find observations for patients named "John"
GET [base]/Observation?subject:Patient.name=John

# Reverse chaining - find patients with a specific observation code
GET [base]/Patient?_has:Observation:subject:code=8480-6

# Missing modifier - find resources without a reference
GET [base]/Observation?subject:missing=true
GET [base]/Observation?subject:missing=false
```

---

### 6. **Quantity**

Searches on Quantity datatypes with optional units.

**Syntax:**
```
[parameter]=[prefix][number]
[parameter]=[prefix][number]|[system]|[code]
[parameter]=[prefix][number]||[code]
```

**Examples:**
```http
# Value only
GET [base]/Observation?value-quantity=5.4

# Value with system and code
GET [base]/Observation?value-quantity=5.4|http://unitsofmeasure.org|mg

# Value with code only
GET [base]/Observation?value-quantity=5.4||mg

# Greater than
GET [base]/Observation?value-quantity=gt150|http://unitsofmeasure.org|mmol/L

# Less than or equal
GET [base]/Observation?value-quantity=le100|http://unitsofmeasure.org|kg

# Range search
GET [base]/Observation?value-quantity=ge5.0&value-quantity=le10.0

# Blood pressure search
GET [base]/Observation?code=85354-9&component-value-quantity=gt140|http://unitsofmeasure.org|mm[Hg]
```

---

### 7. **URI**

Searches on URI elements with exact or hierarchical matching.

**Examples:**
```http
# Exact URI match
GET [base]/ValueSet?url=http://acme.org/fhir/ValueSet/123

# Below modifier (children/descendants)
GET [base]/ValueSet?url:below=http://acme.org/fhir/

# Above modifier (parents/ancestors)
GET [base]/ValueSet?url:above=http://acme.org/fhir/ValueSet/123/_history/5

# OID search
GET [base]/ValueSet?url=urn:oid:1.2.3.4.5

# Canonical URL
GET [base]/StructureDefinition?url=http://hl7.org/fhir/StructureDefinition/Patient
```

---

### 8. **Composite**

Combines multiple search parameters into a single logical unit.

**Syntax:** Uses `$` to separate components.

**Examples:**
```http
# Code and value combination
GET [base]/Observation?code-value-quantity=http://loinc.org|12907-2$ge150|http://unitsofmeasure.org|mmol/L

# Component code and value
GET [base]/Observation?component-code-value-quantity=http://loinc.org|8480-6$gt140

# Context type and value
GET [base]/Questionnaire?context-type-value=focus$http://snomed.info/sct|408934002

# Use context combinations
GET [base]/PlanDefinition?context-type-quantity=venue$gt100

# Group characteristics
GET [base]/Group?characteristic-value=gender$mixed,owner$Eve

# Multiple composite parameters (AND)
GET [base]/Observation?code-value-quantity=http://loinc.org|2093-3$le5&code-value-quantity=http://loinc.org|2085-9$ge60

# Observation with multiple components
GET [base]/Observation?combo-code-value-quantity=http://loinc.org|8480-6$ge140|http://unitsofmeasure.org|mm[Hg]
```

---

### 9. **Special**

Special search parameters with custom behavior.

**Examples:**
```http
# _filter parameter (advanced filtering)
GET [base]/Observation?_filter=code eq http://loinc.org|1234-5 and subject.name co "peter"

# _text search (narrative search)
GET [base]/Observation?_text=cancer OR metastases

# _content search (full resource search)
GET [base]/Observation?_content=tumor

# _query (named queries)
GET [base]/Patient?_query=current-high-risk&ward=Location/A1

# _has (reverse chaining)
GET [base]/Patient?_has:Observation:patient:code=http://loinc.org|8480-6

# _list (list membership)
GET [base]/Patient?_list=101
GET [base]/AllergyIntolerance?patient=42&_list=$current-allergies

# _in (group/list membership)
GET [base]/Encounter?patient._in=Group/104
```

---

## Search Modifiers

Modifiers change the behavior of search parameters by appending `:modifier` to the parameter name.

### Complete List of Modifiers

#### 1. **:above**

Hierarchical search for parents/ancestors.

**Applicable to:** reference, token, uri

**Examples:**
```http
# Find all observations at a location or its parent locations
GET [base]/Observation?location:above=Location/A101

# Token hierarchy
GET [base]/ValueSet?code:above=http://snomed.info/sct|73211009

# URI hierarchy
GET [base]/ValueSet?url:above=http://acme.org/fhir/ValueSet/123/_history/5
```

---

#### 2. **:below**

Hierarchical search for children/descendants.

**Applicable to:** reference, token, uri

**Examples:**
```http
# Find all observations at a location or its child locations
GET [base]/Observation?location:below=Location/BuildingA

# Find conditions below a SNOMED CT code
GET [base]/Condition?code:below=http://snomed.info/sct|126851005

# URI descendants
GET [base]/ValueSet?url:below=http://acme.org/fhir/
```

---

#### 3. **:contains**

Substring matching (case-insensitive).

**Applicable to:** string, uri

**Examples:**
```http
# Find patients with "son" anywhere in family name
GET [base]/Patient?family:contains=son
# Matches: "Son", "Sonder", "Erikson", "Samsonite"

# Address contains
GET [base]/Patient?address:contains=Main
# Matches: "123 Main St", "Main Street", "Omain Ave"

# Organization name contains
GET [base]/Organization?name:contains=health
```

---

#### 4. **:exact**

Exact string match (case-sensitive).

**Applicable to:** string

**Examples:**
```http
# Exact family name (case-sensitive)
GET [base]/Patient?family:exact=Smith
# Matches: "Smith"
# Does NOT match: "smith", "SMITH", "Smithe"

# Exact given name
GET [base]/Patient?given:exact=John

# Exact address
GET [base]/Patient?address:exact=123 Main Street
```

---

#### 5. **:identifier**

Search by identifier in Reference elements.

**Applicable to:** reference

**Examples:**
```http
# Find observations by subject identifier
GET [base]/Observation?subject:identifier=http://example.org/mrn|12345

# Find encounters by participant identifier
GET [base]/Encounter?participant:identifier=http://hospital.org/employee|E98765

# Find medication requests by requester identifier
GET [base]/MedicationRequest?requester:identifier=|NPI-12345
```

---

#### 6. **:in**

Test if value is in a ValueSet.

**Applicable to:** token

**Examples:**
```http
# Find conditions in a specific value set
GET [base]/Condition?code:in=http://acme.org/fhir/ValueSet/cardiac-conditions

# SNOMED CT value set
GET [base]/Condition?code:in=http://snomed.info/sct?fhir_vs=isa/126851005

# Procedure codes in a value set
GET [base]/Procedure?code:in=ValueSet/surgical-procedures
```

---

#### 7. **:iterate**

Apply inclusion directive to included resources.

**Applicable to:** special use with _include/_revinclude

**Examples:**
```http
# Include observations, then include their subjects
GET [base]/DiagnosticReport?_include=DiagnosticReport:result&_include:iterate=Observation:subject

# Multi-level includes
GET [base]/Encounter?_include=Encounter:patient&_include:iterate=Patient:organization
```

---

#### 8. **:missing**

Search for presence or absence of a value.

**Applicable to:** all single-element parameters

**Examples:**
```http
# Find patients without a birthdate
GET [base]/Patient?birthdate:missing=true

# Find patients with a birthdate
GET [base]/Patient?birthdate:missing=false

# Find observations without a performer
GET [base]/Observation?performer:missing=true

# Find conditions with a category
GET [base]/Condition?category:missing=false

# Find medications without a manufacturer
GET [base]/Medication?manufacturer:missing=true
```

---

#### 9. **:not**

Negation for token searches.

**Applicable to:** token

**Examples:**
```http
# Find non-male patients
GET [base]/Patient?gender:not=male
# Includes: female, other, unknown, and patients without gender

# Find active patients (not inactive)
GET [base]/Patient?active:not=false

# Exclude specific codes
GET [base]/Observation?code:not=http://loinc.org|8480-6

# Not in a specific category
GET [base]/Condition?category:not=encounter-diagnosis
```

---

#### 10. **:not-in**

Test if value is NOT in a ValueSet.

**Applicable to:** token

**Examples:**
```http
# Find conditions not in a value set
GET [base]/Condition?code:not-in=http://acme.org/fhir/ValueSet/excluded-conditions

# SNOMED CT exclusion
GET [base]/Condition?code:not-in=http://snomed.info/sct?fhir_vs=isa/404684003
```

---

#### 11. **:of-type**

Search identifiers by type.

**Applicable to:** token (Identifier only)

**Format:** `system|code|value`

**Examples:**
```http
# Medical Record Number search
GET [base]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|12345

# Social Security Number
GET [base]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|SS|123-45-6789

# Driver's License
GET [base]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|DL|D1234567

# National Provider Identifier
GET [base]/Practitioner?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|NPI|1234567890
```

---

#### 12. **:text**

Search on text/display values.

**Applicable to:** reference, token

**Examples:**
```http
# Find conditions by code text/display
GET [base]/Condition?code:text=headache
# Matches codes with display like "Headache", "Headache disorder", "Acute headache"

# Find patients by language text
GET [base]/Patient?language:text=Spanish

# Find observations by code display
GET [base]/Observation?code:text=blood pressure

# Reference display search
GET [base]/Observation?performer:text=Dr. Smith
```

---

#### 13. **:text-advanced**

Advanced text search with multiple words, thesaurus, proximity.

**Applicable to:** reference, token

**Examples:**
```http
# Advanced code text search
GET [base]/Condition?code:text-advanced=diabetes AND type 2

# Complex text search
GET [base]/Observation?code:text-advanced=(blood OR serum) AND glucose

# Practitioner role text
GET [base]/PractitionerRole?specialty:text-advanced=cardiology OR cardiac
```

---

#### 14. **:[type]**

Restrict reference to specific resource type.

**Applicable to:** reference

**Examples:**
```http
# Find observations with Patient subject (not Group)
GET [base]/Observation?subject:Patient=123

# Specific practitioner performer
GET [base]/Observation?performer:Practitioner=P123

# Location participant in encounters
GET [base]/Encounter?participant:Location=L456

# Device used in observations
GET [base]/Observation?device:Device=D789
```

---

#### 15. **:code-text**

Search on code text or display.

**Applicable to:** reference, token

**Examples:**
```http
# Find patients by language code text
GET [base]/Patient?language:code-text=en
# Matches: "en", "en-AU", "en-CA", "en-GB"

# Medication code text
GET [base]/Medication?code:code-text=aspirin

# Allergy code text
GET [base]/AllergyIntolerance?code:code-text=penicillin
```

---

## Search Prefixes

Prefixes control comparison operations for ordered parameter types (number, date, quantity).

| Prefix | Description | Example |
|--------|-------------|---------|
| `eq` | Equal to (default) | `value-quantity=eq5.4` |
| `ne` | Not equal to | `value-quantity=ne5.4` |
| `gt` | Greater than | `date=gt2020-01-01` |
| `lt` | Less than | `date=lt2020-12-31` |
| `ge` | Greater than or equal | `value-quantity=ge100` |
| `le` | Less than or equal | `value-quantity=le200` |
| `sa` | Starts after | `period=sa2020-01-01` |
| `eb` | Ends before | `period=eb2020-12-31` |
| `ap` | Approximately | `value-quantity=ap100` |

### Prefix Examples

```http
# Number prefixes
GET [base]/RiskAssessment?probability=gt0.8
GET [base]/RiskAssessment?probability=le0.5
GET [base]/RiskAssessment?probability=ne1.0

# Date prefixes
GET [base]/Observation?date=gt2020-01-01
GET [base]/Patient?birthdate=le1990-12-31
GET [base]/Encounter?period=sa2020-06-01
GET [base]/Encounter?period=eb2020-12-31

# Quantity prefixes
GET [base]/Observation?value-quantity=gt140|http://unitsofmeasure.org|mm[Hg]
GET [base]/Observation?value-quantity=le100|http://unitsofmeasure.org|kg
GET [base]/Observation?value-quantity=ap98.6|http://unitsofmeasure.org|[degF]

# Combining prefixes (range)
GET [base]/Observation?date=ge2020-01-01&date=le2020-12-31
GET [base]/Observation?value-quantity=ge5.0&value-quantity=le10.0
```

---

## Special Search Conditions

### 1. **Chaining**

Chain search parameters across references.

**Syntax:** `[parameter].[chained-parameter]=[value]`

**Examples:**
```http
# Find observations for patients named "John"
GET [base]/Observation?subject:Patient.name=John

# Find observations for patients in a specific organization
GET [base]/Observation?subject:Patient.organization=Organization/123

# Multiple chaining levels
GET [base]/DiagnosticReport?result.subject:Patient.name=Smith

# Chaining with organization
GET [base]/Encounter?location.organization.name=General Hospital

# Chaining with practitioner
GET [base]/Encounter?participant:Practitioner.name=Dr. Smith

# Complex chaining
GET [base]/MedicationRequest?patient.name=John&patient.birthdate=1980-05-15
```

---

### 2. **Reverse Chaining**

Find resources based on resources that refer to them.

**Syntax:** `_has:[resource]:[reference-param]:[search-param]=[value]`

**Examples:**
```http
# Find patients who have a specific observation
GET [base]/Patient?_has:Observation:patient:code=http://loinc.org|8480-6

# Find patients with high blood pressure observations
GET [base]/Patient?_has:Observation:patient:code-value-quantity=http://loinc.org|8480-6$gt140

# Find organizations with active practitioners
GET [base]/Organization?_has:Practitioner:organization:active=true

# Multiple reverse chaining
GET [base]/Patient?_has:Encounter:patient:date=gt2020-01-01&_has:Observation:patient:code=8480-6

# Complex reverse chaining
GET [base]/Location?_has:Encounter:location:status=in-progress
```

---

### 3. **AND/OR Logic**

**AND Logic:** Use multiple parameters
```http
# Patient named John Smith
GET [base]/Patient?given=John&family=Smith

# Observations with specific code and date range
GET [base]/Observation?code=8480-6&date=ge2020-01-01&date=le2020-12-31
```

**OR Logic:** Use comma-separated values
```http
# Patient named John or Jane
GET [base]/Patient?given=John,Jane

# Patients with multiple identifiers
GET [base]/Patient?identifier=12345,67890

# Multiple condition codes
GET [base]/Condition?code=73211009,44054006
```

**Complex Logic:**
```http
# (John OR Jane) AND (Smith OR Jones)
GET [base]/Patient?given=John,Jane&family=Smith,Jones

# Multiple observations OR'ed together
GET [base]/Observation?code=8480-6,8462-4,8478-0
```

---

### 4. **Advanced Filtering (_filter)**

Complex query expressions using a specialized grammar.

**Examples:**
```http
# Basic filter
GET [base]/Observation?_filter=code eq http://loinc.org|1234-5

# Multiple conditions
GET [base]/Observation?_filter=code eq http://loinc.org|1234-5 and subject.name co "peter"

# OR conditions
GET [base]/Patient?_filter=name co "John" or name co "Jane"

# Complex filter
GET [base]/Observation?_filter=(code eq http://loinc.org|8480-6 and value gt 140) or status eq "final"

# Date filters
GET [base]/Observation?_filter=date ge 2020-01-01 and date le 2020-12-31

# Nested filters
GET [base]/Condition?_filter=code in http://acme.org/ValueSet/123 and onset-date ge 2020-01-01
```

---

## Modifying Search Results

### 1. **_sort**

Control the order of search results.

**Examples:**
```http
# Sort by single parameter (ascending)
GET [base]/Patient?_sort=birthdate

# Sort descending
GET [base]/Patient?_sort=-birthdate

# Multiple sort parameters
GET [base]/Patient?_sort=family,given

# Mixed ascending/descending
GET [base]/Observation?_sort=-date,code

# Sort with search parameters
GET [base]/Patient?family=Smith&_sort=given

# Sort by _lastUpdated
GET [base]/Patient?_sort=-_lastUpdated
```

---

### 2. **_count**

Limit the number of results per page.

**Examples:**
```http
# Return 10 results per page
GET [base]/Patient?_count=10

# Return 50 results
GET [base]/Observation?code=8480-6&_count=50

# Small page size
GET [base]/Patient?_count=5

# Large page size
GET [base]/Patient?_count=100

# Count with other parameters
GET [base]/Observation?date=gt2020-01-01&_count=25
```

---

### 3. **_summary**

Control the amount of information returned.

**Values:** `true`, `text`, `data`, `count`, `false`

**Examples:**
```http
# Summary view (minimal information)
GET [base]/Patient?_summary=true

# Text only (narrative + mandatory elements)
GET [base]/Patient?_summary=text

# Data only (no narrative text)
GET [base]/Observation?_summary=data

# Count only (no resources, just total)
GET [base]/Patient?family=Smith&_summary=count

# Full resources (default)
GET [base]/Patient?_summary=false

# Summary with search
GET [base]/Observation?code=8480-6&_summary=true
```

---

### 4. **_elements**

Request specific elements only.

**Examples:**
```http
# Return only identifier and name
GET [base]/Patient?_elements=identifier,name

# Multiple elements
GET [base]/Observation?_elements=id,status,code,value

# Minimal patient data
GET [base]/Patient?_elements=id,identifier,name,gender,birthdate

# Include nested elements
GET [base]/Observation?_elements=id,code.coding,value

# Elements with search
GET [base]/Patient?family=Smith&_elements=id,name,birthdate
```

---

### 5. **_include**

Include referenced resources in the result.

**Syntax:** `_include=[source-resource]:[search-parameter]`

**Examples:**
```http
# Include patient in observation results
GET [base]/Observation?_include=Observation:patient

# Include multiple related resources
GET [base]/Observation?_include=Observation:patient&_include=Observation:performer

# Include organization with patients
GET [base]/Patient?_include=Patient:organization

# Include all references (wildcard)
GET [base]/Observation?_include=*

# Include specific resource type references
GET [base]/DiagnosticReport?_include=DiagnosticReport:result

# Include with target type
GET [base]/Observation?_include=Observation:subject:Patient

# Multiple includes
GET [base]/Encounter?_include=Encounter:patient&_include=Encounter:participant:Practitioner&_include=Encounter:location
```

---

### 6. **_revinclude**

Include resources that reference the search results.

**Syntax:** `_revinclude=[resource]:[search-parameter]`

**Examples:**
```http
# Find patients and their observations
GET [base]/Patient?_revinclude=Observation:patient

# Find patients and multiple related resources
GET [base]/Patient?name=Smith&_revinclude=Observation:patient&_revinclude=Condition:patient

# Find practitioners and their encounters
GET [base]/Practitioner?_revinclude=Encounter:participant

# Find locations and related encounters
GET [base]/Location?_revinclude=Encounter:location

# Find organizations and related patients
GET [base]/Organization?_revinclude=Patient:organization

# Combine with _include
GET [base]/Patient?_include=Patient:organization&_revinclude=Observation:patient
```

---

### 7. **_include:iterate**

Apply _include to included resources recursively.

**Examples:**
```http
# Include diagnostic report results, then include result subjects
GET [base]/DiagnosticReport?_include=DiagnosticReport:result&_include:iterate=Observation:subject

# Multi-level includes - encounters -> patients -> organizations
GET [base]/Encounter?_include=Encounter:patient&_include:iterate=Patient:organization

# Include medication request medications and their ingredients
GET [base]/MedicationRequest?_include=MedicationRequest:medication&_include:iterate=Medication:ingredient
```

---

### 8. **_total**

Control whether to return the total count of matching resources.

**Values:** `none`, `estimate`, `accurate`

**Examples:**
```http
# Don't return total
GET [base]/Patient?_total=none

# Return estimated total
GET [base]/Patient?_total=estimate

# Return accurate total
GET [base]/Patient?family=Smith&_total=accurate

# Total with paging
GET [base]/Patient?_count=10&_total=accurate
```

---

### 9. **_maxresults**

Limit the total number of results returned across all pages.

**Examples:**
```http
# Maximum 100 results total
GET [base]/Patient?_maxresults=100

# Combined with count
GET [base]/Patient?_count=10&_maxresults=50

# Limited results with search
GET [base]/Observation?code=8480-6&_maxresults=200
```

---

### 10. **_score**

Include relevance score for each result.

**Examples:**
```http
# Return relevance scores
GET [base]/Patient?name=Smith&_score=true

# Text search with scoring
GET [base]/Patient?_text=diabetes&_score=true
```

---

### 11. **_contained**

Control whether to include contained resources.

**Values:** `true`, `false`, `both`

**Examples:**
```http
# Don't return contained resources (default)
GET [base]/Observation?_contained=false

# Return only container resources
GET [base]/Observation?_contained=true

# Return both contained and container
GET [base]/Observation?_contained=both

# Specify container type
GET [base]/Observation?_contained=true&_containedType=container
```

---

### 12. **_graph**

Include resources according to a GraphDefinition.

**Examples:**
```http
# Use named graph definition
GET [base]/Patient?_graph=patientGraph

# Graph by ID
GET [base]/Encounter?_graph=GraphDefinition/encounterDetails
```

---

## Standard Parameters

Standard parameters that work across all resource types:

### 1. **_id**

Search by logical ID.

```http
GET [base]/Patient?_id=123
GET [base]/Patient?_id=123,456,789
GET [base]/Observation?_id=obs-1
```

---

### 2. **_lastUpdated**

Search by last update timestamp.

```http
# Updated after a specific date
GET [base]/Patient?_lastUpdated=gt2020-01-01

# Updated within a date range
GET [base]/Patient?_lastUpdated=ge2020-01-01&_lastUpdated=le2020-12-31

# Recently updated (last hour)
GET [base]/Patient?_lastUpdated=gt2024-01-15T10:00:00Z

# Not updated after a date
GET [base]/Patient?_lastUpdated=lt2020-01-01
```

---

### 3. **_profile**

Search by profile conformance.

```http
# Resources conforming to a specific profile
GET [base]/Patient?_profile=http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient

# Multiple profiles
GET [base]/Observation?_profile=http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab

# Profile with other search parameters
GET [base]/Patient?_profile=http://example.org/StructureDefinition/custom-patient&active=true
```

---

### 4. **_security**

Search by security labels.

```http
# Resources with specific security label
GET [base]/Patient?_security=http://terminology.hl7.org/CodeSystem/v3-Confidentiality|R

# Multiple security labels
GET [base]/Patient?_security=http://terminology.hl7.org/CodeSystem/v3-Confidentiality|N,R,V

# Restricted access resources
GET [base]/Observation?_security=http://terminology.hl7.org/CodeSystem/v3-ActCode|RESCOMPT
```

---

### 5. **_source**

Search by source URI.

```http
# Resources from a specific source
GET [base]/Patient?_source=http://example.org/Organization/123

# Source with other parameters
GET [base]/Observation?_source=http://lab.example.org&code=8480-6
```

---

### 6. **_tag**

Search by resource tags.

```http
# Resources with specific tag
GET [base]/Patient?_tag=http://example.org/codes|VIP

# Multiple tags (OR)
GET [base]/Patient?_tag=urgent,priority

# Tagged resources with search
GET [base]/Observation?_tag=http://example.org/tags|reviewed&status=final
```

---

### 7. **_text**

Search narrative text.

```http
# Search in resource narrative
GET [base]/Patient?_text=diabetes

# Complex text search
GET [base]/Observation?_text=blood pressure OR hypertension

# Text search with filters
GET [base]/Condition?_text=cancer&clinical-status=active
```

---

### 8. **_content**

Full-text search across entire resource.

```http
# Search entire resource content
GET [base]/Patient?_content=John Smith

# Multiple terms
GET [base]/Observation?_content=glucose AND elevated

# Content search with other parameters
GET [base]/Condition?_content=metastases&onset-date=ge2020-01-01
```

---

### 9. **_type**

Filter by resource type (for multi-resource searches).

```http
# Search across multiple types
GET [base]?_type=Patient,Practitioner&name=Smith

# Type with other parameters
GET [base]?_type=Observation,DiagnosticReport&date=gt2020-01-01

# All resources updated recently
GET [base]?_type=*&_lastUpdated=gt2024-01-01
```

---

### 10. **_language**

Filter by resource language.

```http
# Spanish resources
GET [base]/Questionnaire?_language=es

# English resources
GET [base]/Patient?_language=en

# Multiple languages
GET [base]/Questionnaire?_language=en,es,fr
```

---

### 11. **_has** (Reverse Chaining)

See Reverse Chaining section above.

---

### 12. **_list**

Search by list membership.

```http
# Resources in a specific list
GET [base]/Patient?_list=101

# Functional list (current allergies)
GET [base]/AllergyIntolerance?patient=42&_list=$current-allergies

# Multiple lists
GET [base]/Patient?_list=101,102,103
```

---

### 13. **_in**

Test group/list membership.

```http
# Encounters for patients in a group
GET [base]/Encounter?patient._in=Group/104

# Observations for list members
GET [base]/Observation?subject._in=List/patients-cohort

# Resources related to care team members
GET [base]/Encounter?participant._in=CareTeam/team-a
```

---

## Composite Parameters

Composite parameters allow searching on multiple related values together.

### Common Composite Parameters

#### 1. **code-value-quantity**

Search observations by code and value together.

```http
# Sodium observation with value > 150
GET [base]/Observation?code-value-quantity=http://loinc.org|2951-2$gt150|http://unitsofmeasure.org|mmol/L

# Blood pressure with value range
GET [base]/Observation?code-value-quantity=http://loinc.org|8480-6$ge140|http://unitsofmeasure.org|mm[Hg]

# Multiple code-value combinations
GET [base]/Observation?code-value-quantity=http://loinc.org|2093-3$le5,http://loinc.org|2085-9$ge60
```

---

#### 2. **code-value-concept**

Search by code and coded value.

```http
# Find observations with specific code and interpretation
GET [base]/Observation?code-value-concept=http://loinc.org|8480-6$http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation|H

# Condition with specific code and severity
GET [base]/Condition?code-value-concept=http://snomed.info/sct|73211009$http://snomed.info/sct|24484000
```

---

#### 3. **component-code-value-quantity**

Search observation components.

```http
# Blood pressure with systolic > 140
GET [base]/Observation?component-code-value-quantity=http://loinc.org|8480-6$gt140|http://unitsofmeasure.org|mm[Hg]

# Panel observation with specific component values
GET [base]/Observation?component-code-value-quantity=http://loinc.org|8462-4$gt90|http://unitsofmeasure.org|mm[Hg]
```

---

#### 4. **context-type-value**

Search by use context type and value.

```http
# Questionnaires for specific clinical focus
GET [base]/Questionnaire?context-type-value=focus$http://snomed.info/sct|408934002

# Plan definitions with specific use context
GET [base]/PlanDefinition?context-type-value=venue$http://snomed.info/sct|22232009
```

---

#### 5. **context-type-quantity**

Search by use context type and quantity value.

```http
# Resources with age context > 18
GET [base]/PlanDefinition?context-type-quantity=age$gt18|http://unitsofmeasure.org|a

# Activity definitions with specific user context
GET [base]/ActivityDefinition?context-type-quantity=setting$ge10|http://unitsofmeasure.org|1
```

---

#### 6. **characteristic-value**

Search groups by characteristic.

```http
# Groups with specific gender
GET [base]/Group?characteristic-value=gender$mixed

# Multiple characteristics
GET [base]/Group?characteristic-value=gender$female,age-range$adult
```

---

## Advanced Examples

### 1. **Complex Patient Search**

```http
# Find active female patients born between 1970-1990, living in Boston
GET [base]/Patient?active=true&gender=female&birthdate=ge1970-01-01&birthdate=le1990-12-31&address-city=Boston

# Patients with specific identifier type and name
GET [base]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|12345&name=Smith

# Patients in an organization with phone number
GET [base]/Patient?organization=Organization/123&phone=555-1234&_include=Patient:organization
```

---

### 2. **Observation Search Scenarios**

```http
# Blood pressure observations for a patient with high values
GET [base]/Observation?patient=Patient/123&code=85354-9&component-code-value-quantity=http://loinc.org|8480-6$gt140|http://unitsofmeasure.org|mm[Hg]

# Lab results updated in the last 30 days with abnormal values
GET [base]/Observation?category=laboratory&_lastUpdated=ge2024-01-01&interpretation=http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation|A

# Vital signs for encounters in a date range
GET [base]/Observation?category=vital-signs&encounter.period=ge2024-01-01&encounter.period=le2024-01-31

# All observations for patients with diabetes
GET [base]/Observation?patient._has:Condition:patient:code=http://snomed.info/sct|73211009
```

---

### 3. **Encounter Search Scenarios**

```http
# In-progress encounters at a specific location
GET [base]/Encounter?status=in-progress&location=Location/A101&_include=Encounter:patient

# Encounters for a practitioner in the last month
GET [base]/Encounter?participant:Practitioner=Practitioner/P123&date=ge2024-01-01&_revinclude=Observation:encounter

# Emergency encounters with specific service type
GET [base]/Encounter?class=http://terminology.hl7.org/CodeSystem/v3-ActCode|EMER&service-type=http://snomed.info/sct|50849002

# Encounters at location or its child locations
GET [base]/Encounter?location:below=Location/BuildingA&status=finished
```

---

### 4. **Medication Search Scenarios**

```http
# Active medication requests for a patient
GET [base]/MedicationRequest?patient=Patient/123&status=active&_include=MedicationRequest:medication

# Medications by ingredient code
GET [base]/Medication?ingredient-code=http://www.nlm.nih.gov/research/umls/rxnorm|161

# Medication administrations in date range with reason
GET [base]/MedicationAdministration?subject=Patient/123&effective-time=ge2024-01-01&effective-time=le2024-01-31&reason-reference=Condition/456

# Medication statements with adherence issues
GET [base]/MedicationStatement?patient=Patient/123&adherence=http://terminology.hl7.org/CodeSystem/medication-statement-adherence|not-taking
```

---

### 5. **Condition Search Scenarios**

```http
# Active conditions for a patient in a category
GET [base]/Condition?patient=Patient/123&clinical-status=active&category=encounter-diagnosis

# Conditions with onset in specific period
GET [base]/Condition?patient=Patient/123&onset-date=ge2020-01-01&onset-date=le2020-12-31

# Conditions in a value set with high severity
GET [base]/Condition?code:in=http://acme.org/fhir/ValueSet/chronic-diseases&severity=http://snomed.info/sct|24484000

# Conditions with verification status
GET [base]/Condition?patient=Patient/123&verification-status=confirmed&_include=Condition:asserter
```

---

### 6. **Diagnostic Report Search Scenarios**

```http
# Final diagnostic reports for a patient with results
GET [base]/DiagnosticReport?patient=Patient/123&status=final&_include=DiagnosticReport:result

# Reports by category in date range
GET [base]/DiagnosticReport?patient=Patient/123&category=LAB&date=ge2024-01-01&_include=DiagnosticReport:performer

# Reports based on service request
GET [base]/DiagnosticReport?based-on=ServiceRequest/789&_include=DiagnosticReport:result&_include:iterate=Observation:performer
```

---

### 7. **Appointment/Schedule Search**

```http
# Available slots for a practitioner
GET [base]/Slot?schedule.actor=Practitioner/P123&status=free&start=ge2024-02-01&start=le2024-02-29

# Appointments for a patient
GET [base]/Appointment?actor=Patient/123&date=ge2024-01-01&status=booked

# Schedules for a location and service type
GET [base]/Schedule?actor=Location/L456&service-type=http://snomed.info/sct|17561000
```

---

### 8. **Care Team/Plan Search**

```http
# Active care teams for a patient
GET [base]/CareTeam?patient=Patient/123&status=active&_include=CareTeam:participant

# Care plans in a date period with category
GET [base]/CarePlan?patient=Patient/123&date=ge2024-01-01&category=assess-plan&_include=CarePlan:care-team

# Goals for a patient's care plan
GET [base]/Goal?subject=Patient/123&_has:CarePlan:goal:status=active
```

---

### 9. **Document Search**

```http
# Documents by type and date
GET [base]/DocumentReference?patient=Patient/123&type=http://loinc.org|18842-5&date=ge2024-01-01

# Documents by category and security
GET [base]/DocumentReference?patient=Patient/123&category=clinical-note&security-label=http://terminology.hl7.org/CodeSystem/v3-Confidentiality|R

# Compositions with specific section
GET [base]/Composition?patient=Patient/123&section=http://loinc.org|48765-2
```

---

### 10. **Multi-Resource Searches**

```http
# Search patients and practitioners named Smith
GET [base]?_type=Patient,Practitioner&name=Smith

# Recent updates across multiple resource types
GET [base]?_type=Patient,Observation,Condition&_lastUpdated=gt2024-01-01&_count=20

# Text search across resources
GET [base]?_type=Patient,Practitioner,Organization&_text=cardiology
```

---

### 11. **Advanced Filtering Examples**

```http
# Complex filter with multiple conditions
GET [base]/Observation?_filter=((code eq http://loinc.org|8480-6 and value gt 140) or (code eq http://loinc.org|8462-4 and value gt 90)) and status eq "final"

# Date range filter with code
GET [base]/Observation?_filter=code eq http://loinc.org|2093-3 and date ge 2024-01-01 and date le 2024-12-31 and value le 5.0

# Patient filter with complex logic
GET [base]/Patient?_filter=(name co "Smith" or identifier eq "12345") and birthdate le 1990-12-31 and active eq true
```

---

### 12. **Paging Examples**

```http
# First page of results
GET [base]/Patient?family=Smith&_count=20

# Using 'next' link from Bundle
GET [base]/Patient?family=Smith&_count=20&__page=2

# Specific page with sorting
GET [base]/Observation?patient=Patient/123&_sort=-date&_count=50

# All pages with total count
GET [base]/Patient?family=Smith&_count=10&_total=accurate
```

---

### 13. **Including Related Resources**

```http
# Observations with patient and performer
GET [base]/Observation?code=8480-6&_include=Observation:patient&_include=Observation:performer

# Encounters with all participants
GET [base]/Encounter?date=ge2024-01-01&_include=Encounter:patient&_include=Encounter:participant:Practitioner&_include=Encounter:location

# Diagnostic reports with full chain
GET [base]/DiagnosticReport?patient=Patient/123&_include=DiagnosticReport:result&_include:iterate=Observation:performer&_include:iterate=Observation:specimen

# Patient with organization and reverse includes
GET [base]/Patient?_id=123&_include=Patient:organization&_revinclude=Observation:patient&_revinclude=Condition:patient&_revinclude=Encounter:patient
```

---

### 14. **Performance Optimization Examples**

```http
# Summary only for large result sets
GET [base]/Patient?_summary=true&_count=100

# Specific elements only
GET [base]/Patient?family=Smith&_elements=id,identifier,name,birthdate

# Count only (no resources)
GET [base]/Observation?patient=Patient/123&_summary=count

# Data without narrative
GET [base]/Observation?code=8480-6&_summary=data&_count=100
```

---

### 15. **Real-World Clinical Scenarios**

#### Scenario: Find High-Risk Patients

```http
# Patients with diabetes and high recent HbA1c
GET [base]/Patient?_has:Condition:patient:code=http://snomed.info/sct|73211009&_has:Observation:patient:code-value-quantity=http://loinc.org|4548-4$gt9.0

# Patients with hypertension and recent high BP
GET [base]/Patient?_has:Condition:patient:code=http://snomed.info/sct|38341003&_has:Observation:patient:component-code-value-quantity=http://loinc.org|8480-6$gt140
```

#### Scenario: Care Coordination

```http
# All active care elements for a patient
GET [base]/Patient?_id=123&_revinclude=Condition:patient&_revinclude=MedicationRequest:patient&_revinclude=AllergyIntolerance:patient&_revinclude=CarePlan:patient&_revinclude=CareTeam:patient

# Recent encounters with observations
GET [base]/Encounter?patient=Patient/123&date=ge2024-01-01&_revinclude=Observation:encounter&_include=Encounter:participant
```

#### Scenario: Quality Measures

```http
# Patients due for diabetes screening
GET [base]/Patient?active=true&birthdate=le2000-01-01&_has:Observation:patient:code=http://loinc.org|4548-4&_has:Observation:patient:date=lt2023-01-01

# Completed preventive services
GET [base]/Procedure?patient=Patient/123&category=http://snomed.info/sct|409073007&status=completed&date=ge2023-01-01
```

#### Scenario: Medication Reconciliation

```http
# All active medications across contexts
GET [base]/MedicationRequest?patient=Patient/123&status=active&_include=MedicationRequest:medication

# Medication history
GET [base]/MedicationStatement?patient=Patient/123&effective-time=ge2024-01-01&_include=MedicationStatement:medication

# Recent administrations
GET [base]/MedicationAdministration?patient=Patient/123&effective-time=ge2024-01-01&_include=MedicationAdministration:medication
```

---

## Search Tips and Best Practices

### 1. **Use Specific Parameters**
```http
# Less efficient
GET [base]/Patient?_text=Smith

# More efficient
GET [base]/Patient?family=Smith
```

### 2. **Limit Result Sets**
```http
# Always use _count for large queries
GET [base]/Observation?_count=50

# Use date ranges
GET [base]/Observation?date=ge2024-01-01&date=le2024-01-31
```

### 3. **Use _summary for Large Resources**
```http
# Get minimal data first
GET [base]/DiagnosticReport?_summary=true

# Then fetch full resources as needed
GET [base]/DiagnosticReport/123
```

### 4. **Use _include Wisely**
```http
# Include only what you need
GET [base]/Observation?_include=Observation:patient

# Avoid excessive includes
# GET [base]/Observation?_include=*  # Use sparingly
```

### 5. **Combine Searches Efficiently**
```http
# Single request with includes
GET [base]/Patient?_id=123&_revinclude=Observation:patient&_revinclude=Condition:patient

# Instead of multiple separate requests
```

### 6. **Use Appropriate Modifiers**
```http
# Use :missing to find gaps
GET [base]/Patient?birthdate:missing=true

# Use :not for exclusions
GET [base]/Patient?gender:not=male

# Use :contains for flexible matching
GET [base]/Patient?address:contains=Main
```

### 7. **Cache Common Searches**
```http
# Cacheable searches
GET [base]/CodeSystem?url=http://loinc.org

# Use ETag/Last-Modified headers
```

### 8. **Handle Errors Gracefully**
```http
# Check for OperationOutcome in search results
# Handle empty result sets
# Validate parameter combinations
```

---

## URL Encoding

Remember to URL encode special characters in search values:

```http
# Before encoding
GET [base]/Patient?identifier=http://example.org/ids|12345

# After encoding
GET [base]/Patient?identifier=http%3A%2F%2Fexample.org%2Fids%7C12345

# Common encodings:
# | → %7C
# : → %3A  
# / → %2F
# # → %23
# ? → %3F
# & → %26
# = → %3D
# space → %20 or +
```

---

## Conclusion

FHIR Search provides a powerful and flexible mechanism for querying healthcare data. This guide covers:

- **Basic search syntax** for all resource types
- **8 parameter types** with extensive examples
- **15+ modifiers** for refined searches  
- **9 prefixes** for comparisons
- **Special conditions** like chaining and reverse chaining
- **Result modification** with _include, _sort, _count, etc.
- **Standard parameters** across all resources
- **Composite searches** for complex queries
- **Real-world scenarios** for clinical use cases

For the most up-to-date information, always refer to the official FHIR specification at https://www.hl7.org/fhir/search.html

---

## Quick Reference Card

### Common Search Patterns

| Pattern | Example |
|---------|---------|
| Simple search | `GET [base]/Patient?name=Smith` |
| Multiple parameters (AND) | `GET [base]/Patient?family=Smith&given=John` |
| Multiple values (OR) | `GET [base]/Patient?name=Smith,Jones` |
| Date range | `GET [base]/Observation?date=ge2024-01-01&date=le2024-12-31` |
| Reference | `GET [base]/Observation?subject=Patient/123` |
| Chaining | `GET [base]/Observation?subject:Patient.name=Smith` |
| Reverse chaining | `GET [base]/Patient?_has:Observation:patient:code=8480-6` |
| Include | `GET [base]/Observation?_include=Observation:patient` |
| Paging | `GET [base]/Patient?_count=20` |
| Sorting | `GET [base]/Patient?_sort=family,given` |
| Summary | `GET [base]/Patient?_summary=true` |
| Elements | `GET [base]/Patient?_elements=id,name,birthdate` |
| Missing values | `GET [base]/Patient?birthdate:missing=true` |
| Text search | `GET [base]/Patient?_text=diabetes` |
| Filter | `GET [base]/Observation?_filter=code eq http://loinc.org\|8480-6 and value gt 140` |

---

**Document Version:** 1.0  
**Based on:** FHIR R5 Specification  
**Last Updated:** May 2026
