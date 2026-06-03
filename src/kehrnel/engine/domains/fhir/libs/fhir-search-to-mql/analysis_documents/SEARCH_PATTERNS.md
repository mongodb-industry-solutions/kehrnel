# FHIR Schedule & Appointment: Search Patterns and Data Structure Optimization Strategies

## Executive Summary

This document analyzes search patterns for FHIR Schedule and Appointment resources and presents multiple data structure strategies optimized for MongoDB at scale (millions of records). Each strategy is evaluated for query performance, storage efficiency, and operational complexity.

**Key Resources Analyzed:**
- **Schedule**: Container for time slots representing practitioner/service availability
- **Slot**: Bookable time intervals within a schedule
- **Appointment**: Confirmed booking linking patients, practitioners, locations, and slots
- **AppointmentResponse**: Participant responses to appointment requests

---

## Common Search Patterns for Schedule & Appointment Resources

### 1. Schedule Search Patterns

#### SP-1: Find Schedules by Practitioner/Actor
**Use Case**: Display all schedules for a specific practitioner or service  
**Query Pattern**: `Schedule.actor.reference = "Practitioner/{id}"`  
**Frequency**: Very High (every practitioner dashboard load)  
**Data Access**: Single document lookup with actor reference

#### SP-2: Find Available Schedules by Service Type & Specialty
**Use Case**: Patient searching for specific type of care  
**Query Pattern**: 
```javascript
{
  'serviceType.coding.code': 'service-code',
  'specialty.coding.code': 'specialty-code',
  'active': true
}
```
**Frequency**: High (patient booking flows)  
**Data Access**: Index scan on service metadata

#### SP-3: Find Active Schedules within Date Range
**Use Case**: Show schedules active during specific period  
**Query Pattern**: 
```javascript
{
  'active': true,
  'planningHorizon.start': { $lte: end_date },
  'planningHorizon.end': { $gte: start_date }
}
```
**Frequency**: High  
**Data Access**: Compound index on active + date range

#### SP-4: Find Schedules by Location
**Use Case**: Show all schedules at a specific facility/room  
**Query Pattern**: `Schedule.actor.reference = "Location/{id}"`  
**Frequency**: Medium  
**Data Access**: Actor reference lookup

---

### 2. Slot Search Patterns

#### SL-1: Find Free Slots for a Schedule
**Use Case**: Show available booking times for a practitioner/service  
**Query Pattern**:
```javascript
{
  'schedule.reference': 'Schedule/{id}',
  'status': 'free',
  'start': { $gte: now },
  'end': { $lte: horizon }
}
```
**Frequency**: Extremely High (every booking search)  
**Data Access**: Compound index on schedule + status + start/end

#### SL-2: Find Available Slots by Service Type & Date Range
**Use Case**: Patient looking for specific service in next N days  
**Query Pattern**:
```javascript
{
  'serviceType.coding.code': 'service-code',
  'status': 'free',
  'start': { $gte: start_date, $lte: end_date }
}
```
**Frequency**: Very High  
**Data Access**: Compound index on serviceType + status + date range

#### SL-3: Find Slots by Specialty & Location
**Use Case**: Find specialists at specific locations  
**Query Pattern**:
```javascript
{
  'specialty.coding.code': 'specialty-code',
  // Requires join to Schedule for location
  'status': 'free'
}
```
**Frequency**: High  
**Data Access**: Index + potential join/denormalization

#### SL-4: Find Overbooked Slots
**Use Case**: Administrative monitoring of scheduling issues  
**Query Pattern**: `{ 'overbooked': true }`  
**Frequency**: Low (admin/reporting)  
**Data Access**: Simple index scan

---

### 3. Appointment Search Patterns

#### AP-1: Find Appointments by Patient
**Use Case**: Patient portal - "my appointments"  
**Query Pattern**:
```javascript
{
  'participant.actor.reference': 'Patient/{id}',
  'status': { $in: ['booked', 'arrived', 'fulfilled', 'pending'] }
}
```
**Frequency**: Extremely High (every patient login)  
**Data Access**: Compound index on participant reference + status

#### AP-2: Find Appointments by Practitioner & Date
**Use Case**: Daily schedule for a doctor  
**Query Pattern**:
```javascript
{
  'participant.actor.reference': 'Practitioner/{id}',
  'start': { $gte: day_start, $lt: day_end }
}
```
**Frequency**: Extremely High (practitioner views)  
**Data Access**: Compound index on participant + start date

#### AP-3: Find Appointments by Status
**Use Case**: Queue management (pending, waitlist, no-shows)  
**Query Pattern**: `{ 'status': 'pending' }`  
**Frequency**: High (operational dashboards)  
**Data Access**: Status index scan

#### AP-4: Find Appointments by Date Range
**Use Case**: Reporting, calendar views, capacity planning  
**Query Pattern**: `{ 'start': { $gte: start, $lte: end } }`  
**Frequency**: Very High  
**Data Access**: Date range index

#### AP-5: Find Appointments by Service Type
**Use Case**: Department-specific scheduling views  
**Query Pattern**: `{ 'serviceType.coding.code': 'code' }`  
**Frequency**: Medium-High  
**Data Access**: Service type index

#### AP-6: Find Recurring Appointments (Series)
**Use Case**: Manage recurring visit series  
**Query Pattern**: 
```javascript
{
  $or: [
    { 'recurrenceTemplate': { $exists: true } },
    { 'originatingAppointment.reference': 'Appointment/{id}' }
  ]
}
```
**Frequency**: Medium  
**Data Access**: Compound index on recurrence fields

#### AP-7: Find Appointments by Slot
**Use Case**: Check if a slot is actually booked  
**Query Pattern**: `{ 'slot.reference': 'Slot/{id}' }`  
**Frequency**: Medium  
**Data Access**: Slot reference index

#### AP-8: Complex Multi-Factor Search
**Use Case**: "Find available cardiology appointments with Dr. Smith at Main Hospital next week"  
**Query Pattern**:
```javascript
{
  'participant.actor.reference': 'Practitioner/smith-123',
  'specialty.coding.code': '394579002',
  'start': { $gte: week_start, $lte: week_end },
  'status': 'free' // via Slot lookup
}
```
**Frequency**: High  
**Data Access**: Multiple indexes + potential aggregation

---

## Data Structure Optimization Strategies

### Strategy 1: Fully Normalized (Strict FHIR Compliance)

#### Collection Structure
**Collections Required: 4**
1. `Schedule` - One collection
2. `Slot` - One collection  
3. `Appointment` - One collection
4. `AppointmentResponse` - One collection

#### Data Model
```javascript
// Schedule Collection
{
  "_id": "schedule-001",
  "resourceType": "Schedule",
  "active": true,
  "actor": [{ "reference": "Practitioner/prac-123" }],
  "serviceType": [...],
  "specialty": [...],
  "planningHorizon": { "start": "2026-05-01", "end": "2026-06-30" }
}

// Slot Collection  
{
  "_id": "slot-001",
  "resourceType": "Slot",
  "schedule": { "reference": "Schedule/schedule-001" },
  "status": "free",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  "serviceType": [...]
}

// Appointment Collection
{
  "_id": "appt-001",
  "resourceType": "Appointment",
  "status": "booked",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  "participant": [
    { "actor": { "reference": "Patient/pat-123" }, "status": "accepted" },
    { "actor": { "reference": "Practitioner/prac-123" }, "status": "accepted" }
  ],
  "slot": [{ "reference": "Slot/slot-001" }]
}
```

#### Index Strategy
```javascript
// Schedule indexes
db.Schedule.createIndex({ "actor.reference": 1 })
db.Schedule.createIndex({ "serviceType.coding.code": 1, "specialty.coding.code": 1 })
db.Schedule.createIndex({ "active": 1, "planningHorizon.start": 1, "planningHorizon.end": 1 })

// Slot indexes
db.Slot.createIndex({ "schedule.reference": 1, "status": 1, "start": 1 })
db.Slot.createIndex({ "status": 1, "start": 1, "end": 1 })
db.Slot.createIndex({ "serviceType.coding.code": 1, "status": 1, "start": 1 })
db.Slot.createIndex({ "start": 1 })

// Appointment indexes
db.Appointment.createIndex({ "participant.actor.reference": 1, "status": 1 })
db.Appointment.createIndex({ "start": 1, "end": 1 })
db.Appointment.createIndex({ "status": 1 })
db.Appointment.createIndex({ "slot.reference": 1 })
db.Appointment.createIndex({ "originatingAppointment.reference": 1 })
```

#### Benefits

1. **Perfect FHIR Compliance**
   - Each resource maintains its canonical FHIR structure
   - Can be directly validated against FHIR schemas
   - Easy integration with FHIR servers and validators
   - Clear separation of concerns

2. **Resource Independence**
   - Each resource can be updated independently
   - No data duplication means no synchronization issues
   - Clean deletion semantics (cascade or orphan management)
   - Simple versioning and audit trails per resource

3. **Storage Efficiency**
   - No redundant data storage
   - Each piece of information stored once
   - Optimal for write-heavy scenarios with minimal reads
   - Efficient for resource-level CRUD operations

4. **Flexibility**
   - Easy to add new resource types
   - Schema changes isolated to specific collections
   - Can leverage MongoDB transactions across collections
   - Straightforward to implement FHIR RESTful operations

5. **Standards Alignment**
   - Matches FHIR specification exactly
   - Simplifies migration to/from other FHIR systems
   - Development team can follow FHIR documentation directly
   - Interoperability with FHIR ecosystems

#### Trade-offs

- **Multiple Lookups Required**: Most searches need 2-3 database queries (Schedule → Slot → Appointment)
- **Join Complexity**: Complex queries require $lookup aggregations which are expensive
- **Higher Latency**: Network round-trips for each collection access
- **Index Overhead**: More collections = more indexes to maintain
- **Read Performance**: Not optimal for read-heavy booking searches

#### Best For
- FHIR server implementations
- Systems requiring strict healthcare standards compliance
- Environments with strong consistency requirements
- Multi-system integration scenarios
- Audit and compliance-heavy environments

---

### Strategy 2: Embedded Denormalization (Read-Optimized)

#### Collection Structure
**Collections Required: 2-3**
1. `Schedule` - Contains embedded Slots
2. `Appointment` - Standalone with embedded participant details
3. `AppointmentResponse` (optional) - Can be embedded in Appointment or standalone

#### Data Model
```javascript
// Schedule Collection (with embedded Slots)
{
  "_id": "schedule-001",
  "resourceType": "Schedule",
  "active": true,
  "actor": [{ "reference": "Practitioner/prac-123" }],
  "actorDetails": {  // Denormalized for faster access
    "practitionerId": "prac-123",
    "practitionerName": "Dr. John Smith",
    "specialty": "Cardiology",
    "locationId": "loc-456",
    "locationName": "Main Hospital - Cardiology Wing"
  },
  "serviceType": [...],
  "specialty": [...],
  "planningHorizon": { "start": "2026-05-01", "end": "2026-06-30" },
  "slots": [  // Embedded slots array
    {
      "id": "slot-001",
      "status": "free",
      "start": "2026-05-10T09:00:00Z",
      "end": "2026-05-10T09:30:00Z",
      "serviceType": [...],
      "appointmentType": [...]
    },
    {
      "id": "slot-002",
      "status": "busy",
      "start": "2026-05-10T09:30:00Z",
      "end": "2026-05-10T10:00:00Z"
    }
    // ... potentially hundreds of slots per schedule
  ],
  "slotSummary": {  // Aggregated for quick lookups
    "totalSlots": 240,
    "freeSlots": 180,
    "busySlots": 50,
    "unavailableSlots": 10,
    "nextAvailableSlot": "2026-05-10T09:00:00Z",
    "lastUpdated": "2026-05-09T14:23:00Z"
  }
}

// Appointment Collection (with denormalized data)
{
  "_id": "appt-001",
  "resourceType": "Appointment",
  "status": "booked",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  
  // Original FHIR references
  "participant": [
    { "actor": { "reference": "Patient/pat-123" }, "status": "accepted" },
    { "actor": { "reference": "Practitioner/prac-123" }, "status": "accepted" },
    { "actor": { "reference": "Location/loc-456" }, "status": "accepted" }
  ],
  "slot": [{ "reference": "Slot/slot-001" }],
  "schedule": { "reference": "Schedule/schedule-001" },
  
  // Denormalized for query optimization
  "patientDetails": {
    "id": "pat-123",
    "name": "Jane Doe",
    "dateOfBirth": "1985-03-15",
    "contactPhone": "+1-555-0123",
    "email": "jane.doe@example.com"
  },
  "practitionerDetails": {
    "id": "prac-123",
    "name": "Dr. John Smith",
    "specialty": "Cardiology",
    "npi": "1234567890"
  },
  "locationDetails": {
    "id": "loc-456",
    "name": "Main Hospital - Cardiology Wing",
    "address": "123 Medical Dr, Room 304"
  },
  "scheduleDetails": {
    "id": "schedule-001",
    "name": "Dr. Smith - Weekday Clinic"
  },
  
  // Fast-access fields
  "patientId": "pat-123",
  "practitionerId": "prac-123",
  "locationId": "loc-456",
  "scheduleId": "schedule-001",
  
  // Pre-computed for reporting
  "dateOnly": "2026-05-10",
  "timeSlot": "morning",  // morning, afternoon, evening
  "dayOfWeek": "Friday",
  "weekNumber": 19,
  "monthYear": "2026-05"
}
```

#### Index Strategy
```javascript
// Schedule indexes
db.Schedule.createIndex({ "actorDetails.practitionerId": 1 })
db.Schedule.createIndex({ "actorDetails.locationId": 1 })
db.Schedule.createIndex({ "serviceType.coding.code": 1, "active": 1 })
db.Schedule.createIndex({ "slots.status": 1, "slots.start": 1 })
db.Schedule.createIndex({ "slotSummary.freeSlots": 1, "slotSummary.nextAvailableSlot": 1 })

// Appointment indexes
db.Appointment.createIndex({ "patientId": 1, "start": 1 })
db.Appointment.createIndex({ "practitionerId": 1, "dateOnly": 1 })
db.Appointment.createIndex({ "locationId": 1, "dateOnly": 1 })
db.Appointment.createIndex({ "dateOnly": 1, "timeSlot": 1 })
db.Appointment.createIndex({ "status": 1, "start": 1 })
db.Appointment.createIndex({ "monthYear": 1, "status": 1 })
db.Appointment.createIndex({ 
  "practitionerId": 1, 
  "start": 1, 
  "status": 1 
})
```

#### Benefits

1. **Exceptional Read Performance**
   - Single query returns all needed data
   - No joins or multiple lookups required
   - Slot search within schedule is document-internal (very fast)
   - Patient appointment view needs only one query
   - Practitioner daily schedule is a single indexed lookup

2. **Reduced Network Round-Trips**
   - All related data in one document
   - Lower latency for complex queries
   - Better user experience for booking flows
   - Fewer database connections needed

3. **Simplified Query Logic**
   - No complex aggregation pipelines
   - Application code is simpler
   - Easier to optimize and debug
   - Reduced cognitive load for developers

4. **Better Caching Opportunities**
   - Complete documents can be cached
   - Higher cache hit rates
   - Reduced database load
   - CDN/edge caching for schedules with slots

5. **Pre-computed Aggregations**
   - slotSummary provides instant stats
   - dateOnly/timeSlot enable fast filtering
   - Reporting queries are faster
   - Dashboard queries highly optimized

6. **Locality of Reference**
   - Related data stored together
   - Better MongoDB memory utilization
   - More efficient disk I/O
   - Improved working set performance

#### Trade-offs

- **Data Duplication**: Patient/Practitioner details repeated across appointments
- **Document Size**: Schedules with embedded slots can grow very large (16MB limit concern)
- **Update Complexity**: Changing practitioner name requires updating many appointments
- **Write Amplification**: Must update denormalized fields in multiple documents
- **Synchronization Overhead**: Need application logic to keep denormalized data consistent
- **Storage Cost**: More disk space required (typically 2-3x normalized)

#### Best For
- High-volume patient booking portals
- Mobile applications requiring fast response times
- Read-heavy workloads (10:1 read:write ratio or higher)
- Real-time availability search
- Patient/practitioner dashboard views
- Systems where storage cost is not primary concern

---

### Strategy 3: Hybrid Approach (Balanced)

#### Collection Structure
**Collections Required: 5-6**
1. `Schedule` - Standalone, metadata only
2. `Slot` - Standalone with selective denormalization
3. `Appointment` - Standalone with selective denormalization
4. `SlotIndex` - Specialized search index collection
5. `AppointmentResponse` - Standalone
6. `AppointmentSummary` (optional) - Aggregated views

#### Data Model
```javascript
// Schedule Collection (Lean)
{
  "_id": "schedule-001",
  "resourceType": "Schedule",
  "active": true,
  "actor": [{ "reference": "Practitioner/prac-123" }],
  "actorId": "prac-123",  // Denormalized for indexing
  "actorType": "Practitioner",
  "actorName": "Dr. John Smith",  // Minimal denormalization
  "serviceType": [...],
  "specialty": [...],
  "planningHorizon": { "start": "2026-05-01", "end": "2026-06-30" },
  "metadata": {
    "slotCount": 240,
    "createdAt": "2026-04-01T10:00:00Z",
    "updatedAt": "2026-05-09T14:23:00Z"
  }
}

// Slot Collection (With Strategic Denormalization)
{
  "_id": "slot-001",
  "resourceType": "Slot",
  "schedule": { "reference": "Schedule/schedule-001" },
  "scheduleId": "schedule-001",  // Denormalized for fast lookup
  "status": "free",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  
  // Selectively denormalized from Schedule
  "scheduleActor": {
    "id": "prac-123",
    "type": "Practitioner",
    "name": "Dr. John Smith"
  },
  "serviceType": [...],
  "specialty": [...],
  
  // Pre-computed for fast filtering
  "dateOnly": "2026-05-10",
  "dayOfWeek": 5,  // 0=Sunday, 6=Saturday
  "timeOfDay": "morning",
  "durationMinutes": 30
}

// SlotIndex Collection (Optimized Search Index)
{
  "_id": "idx-2026-05-10-prac-123",
  "date": "2026-05-10",
  "practitionerId": "prac-123",
  "practitionerName": "Dr. John Smith",
  "locationId": "loc-456",
  "specialty": "Cardiology",
  "serviceType": "consultation",
  "freeSlots": [
    {
      "slotId": "slot-001",
      "start": "2026-05-10T09:00:00Z",
      "end": "2026-05-10T09:30:00Z",
      "duration": 30
    },
    {
      "slotId": "slot-003",
      "start": "2026-05-10T10:00:00Z",
      "end": "2026-05-10T10:30:00Z",
      "duration": 30
    }
  ],
  "totalFreeSlots": 2,
  "firstAvailable": "2026-05-10T09:00:00Z",
  "lastAvailable": "2026-05-10T10:00:00Z",
  "updatedAt": "2026-05-09T14:23:00Z"
}

// Appointment Collection (Balanced Denormalization)
{
  "_id": "appt-001",
  "resourceType": "Appointment",
  "status": "booked",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  
  // Original references
  "participant": [
    { "actor": { "reference": "Patient/pat-123" }, "status": "accepted" },
    { "actor": { "reference": "Practitioner/prac-123" }, "status": "accepted" }
  ],
  "slot": [{ "reference": "Slot/slot-001" }],
  
  // Minimal denormalization for common queries
  "patientId": "pat-123",
  "patientName": "Jane Doe",  // Name only, no full details
  "practitionerId": "prac-123",
  "practitionerName": "Dr. John Smith",
  "locationId": "loc-456",
  
  // Query optimization fields
  "dateOnly": "2026-05-10",
  "participantIds": ["pat-123", "prac-123", "loc-456"]  // Array for efficient $in queries
}

// AppointmentSummary Collection (Pre-aggregated Reports)
{
  "_id": "summary-prac-123-2026-05-10",
  "practitionerId": "prac-123",
  "practitionerName": "Dr. John Smith",
  "date": "2026-05-10",
  "appointments": [
    {
      "appointmentId": "appt-001",
      "patientId": "pat-123",
      "patientName": "Jane Doe",
      "start": "2026-05-10T09:00:00Z",
      "end": "2026-05-10T09:30:00Z",
      "status": "booked",
      "type": "consultation"
    }
  ],
  "statistics": {
    "totalAppointments": 8,
    "booked": 6,
    "cancelled": 1,
    "noShow": 1,
    "utilization": 0.75
  },
  "generatedAt": "2026-05-10T06:00:00Z"
}
```

#### Index Strategy
```javascript
// Schedule indexes (Minimal)
db.Schedule.createIndex({ "actorId": 1, "active": 1 })
db.Schedule.createIndex({ "serviceType.coding.code": 1, "specialty.coding.code": 1 })

// Slot indexes (Comprehensive for search)
db.Slot.createIndex({ "scheduleId": 1, "status": 1, "start": 1 })
db.Slot.createIndex({ "status": 1, "dateOnly": 1, "scheduleActor.id": 1 })
db.Slot.createIndex({ 
  "serviceType.coding.code": 1, 
  "status": 1, 
  "start": 1 
})
db.Slot.createIndex({ "dateOnly": 1, "status": 1 })

// SlotIndex indexes (Ultra-fast availability search)
db.SlotIndex.createIndex({ "date": 1, "practitionerId": 1 })
db.SlotIndex.createIndex({ 
  "date": 1, 
  "specialty": 1, 
  "totalFreeSlots": 1 
})
db.SlotIndex.createIndex({ 
  "practitionerId": 1, 
  "date": 1, 
  "totalFreeSlots": 1 
})

// Appointment indexes (Optimized for common queries)
db.Appointment.createIndex({ "participantIds": 1, "dateOnly": 1 })
db.Appointment.createIndex({ "patientId": 1, "start": 1 })
db.Appointment.createIndex({ "practitionerId": 1, "dateOnly": 1 })
db.Appointment.createIndex({ "status": 1, "start": 1 })

// AppointmentSummary indexes
db.AppointmentSummary.createIndex({ "practitionerId": 1, "date": 1 })
db.AppointmentSummary.createIndex({ "date": 1 })
```

#### Benefits

1. **Optimized for Both Reads and Writes**
   - Fast reads via SlotIndex and selective denormalization
   - Efficient writes by limiting denormalization
   - Balanced performance for real-world workloads
   - Minimal write amplification

2. **Specialized Search Performance**
   - SlotIndex provides sub-second availability queries
   - Pre-aggregated daily slot availability
   - Common searches hit optimized indexes
   - Complex searches still performant via Slot collection

3. **Controlled Data Duplication**
   - Only critical fields denormalized (IDs, names)
   - Reduces storage overhead vs full denormalization
   - Synchronization complexity manageable
   - Full details available via references when needed

4. **Flexible Query Patterns**
   - Simple queries use SlotIndex (fastest)
   - Moderate queries use Slot with denormalized fields
   - Complex queries can still access normalized Schedule
   - Different access patterns don't conflict

5. **Incremental Maintenance**
   - SlotIndex can be rebuilt asynchronously
   - AppointmentSummary updated via batch jobs
   - Real-time data in main collections
   - Eventual consistency acceptable for summaries

6. **Scalability**
   - Can shard Slot collection by date range
   - SlotIndex naturally partitioned by date
   - Appointments can be partitioned by date/patient
   - Each collection scales independently

7. **Practical FHIR Compliance**
   - Core resources remain FHIR-compliant
   - Index collections are internal optimizations
   - Can serve FHIR API from main collections
   - Hybrid model transparent to external consumers

#### Trade-offs

- **Operational Complexity**: More collections to manage and monitor
- **Synchronization Logic**: Need to maintain SlotIndex and summary collections
- **Storage Overhead**: Moderate (1.5-2x normalized)
- **Build/Rebuild Overhead**: SlotIndex requires periodic updates
- **Debugging Complexity**: More moving parts to troubleshoot

#### Best For
- **Production healthcare systems with balanced workloads**
- **High-scale booking platforms (100K+ appointments/day)**
- **Systems requiring both fast reads and reasonable write performance**
- **Organizations with DevOps maturity for managing multiple collections**
- **Real-time booking with reporting requirements**

#### Maintenance Strategy

**SlotIndex Rebuild:**
```javascript
// Nightly rebuild of SlotIndex for next 90 days
// Run as background job at 2 AM
db.SlotIndex.deleteMany({ date: { $gte: today, $lte: today+90 } })

db.Slot.aggregate([
  {
    $match: {
      status: 'free',
      dateOnly: { $gte: today, $lte: today+90 }
    }
  },
  {
    $group: {
      _id: {
        date: '$dateOnly',
        practitionerId: '$scheduleActor.id',
        specialty: '$specialty.coding.code',
        locationId: '$locationId'
      },
      freeSlots: {
        $push: {
          slotId: '$_id',
          start: '$start',
          end: '$end',
          duration: '$durationMinutes'
        }
      },
      totalFreeSlots: { $sum: 1 },
      firstAvailable: { $min: '$start' },
      lastAvailable: { $max: '$start' }
    }
  }
]).forEach(doc => {
  db.SlotIndex.insertOne({
    _id: `idx-${doc._id.date}-${doc._id.practitionerId}`,
    date: doc._id.date,
    practitionerId: doc._id.practitionerId,
    specialty: doc._id.specialty,
    freeSlots: doc.freeSlots,
    totalFreeSlots: doc.totalFreeSlots,
    firstAvailable: doc.firstAvailable,
    lastAvailable: doc.lastAvailable,
    updatedAt: new Date()
  })
})
```

**Incremental SlotIndex Update:**
```javascript
// On slot status change, update SlotIndex in real-time
function updateSlotIndex(slotId, oldStatus, newStatus) {
  const slot = db.Slot.findOne({ _id: slotId })
  const indexId = `idx-${slot.dateOnly}-${slot.scheduleActor.id}`
  
  if (oldStatus === 'free' && newStatus === 'busy') {
    // Slot booked - remove from index
    db.SlotIndex.updateOne(
      { _id: indexId },
      {
        $pull: { freeSlots: { slotId: slotId } },
        $inc: { totalFreeSlots: -1 },
        $set: { updatedAt: new Date() }
      }
    )
  } else if (oldStatus === 'busy' && newStatus === 'free') {
    // Slot freed - add to index
    db.SlotIndex.updateOne(
      { _id: indexId },
      {
        $push: {
          freeSlots: {
            slotId: slot._id,
            start: slot.start,
            end: slot.end,
            duration: slot.durationMinutes
          }
        },
        $inc: { totalFreeSlots: 1 },
        $set: { updatedAt: new Date() }
      }
    )
  }
}
```

---

### Strategy 4: Time-Based Partitioning (High-Scale)

#### Collection Structure
**Collections Required: Dynamic (increases over time)**
- `Schedule` - Single collection
- `Slot_YYYYMM` - One per month (e.g., `Slot_202605`, `Slot_202606`)
- `Appointment_YYYYMM` - One per month
- `AppointmentResponse_YYYYMM` - One per month
- `SlotIndex_YYYYMM` - One per month (optional)
- `HistoricalAppointment` - Archive for old appointments (optional)

#### Data Model
```javascript
// Schedule Collection (Unchanged)
{
  "_id": "schedule-001",
  "resourceType": "Schedule",
  "active": true,
  "actor": [{ "reference": "Practitioner/prac-123" }],
  "actorId": "prac-123",
  "planningHorizon": { "start": "2026-05-01", "end": "2026-12-31" }
}

// Slot_202605 Collection
{
  "_id": "slot-001",
  "resourceType": "Slot",
  "scheduleId": "schedule-001",
  "status": "free",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  "month": "202605",  // Partition key
  "dateOnly": "2026-05-10",
  // ... selective denormalization
}

// Appointment_202605 Collection
{
  "_id": "appt-001",
  "resourceType": "Appointment",
  "status": "booked",
  "start": "2026-05-10T09:00:00Z",
  "end": "2026-05-10T09:30:00Z",
  "month": "202605",  // Partition key
  "patientId": "pat-123",
  "practitionerId": "prac-123",
  // ... rest of appointment data
}

// HistoricalAppointment Collection (Archived)
// Appointments moved here after 6-12 months
{
  "_id": "appt-historical-001",
  "originalId": "appt-001",
  "archivedDate": "2026-11-01T00:00:00Z",
  // ... full appointment data
}
```

#### Routing Logic
```javascript
// Application-level routing
function getAppointmentCollection(date) {
  const monthKey = date.substring(0, 7).replace('-', '')  // "2026-05-10" -> "202605"
  return db.collection(`Appointment_${monthKey}`)
}

function getSlotCollection(date) {
  const monthKey = date.substring(0, 7).replace('-', '')
  return db.collection(`Slot_${monthKey}`)
}

// Search across multiple months
function findAppointments(startDate, endDate, patientId) {
  const collections = getMonthCollectionsBetween(startDate, endDate)
  const results = []
  
  for (const collName of collections) {
    const coll = db.collection(collName)
    const docs = coll.find({
      patientId: patientId,
      dateOnly: { $gte: startDate, $lte: endDate }
    }).toArray()
    results.push(...docs)
  }
  
  return results.sort((a, b) => a.start.localeCompare(b.start))
}
```

#### Index Strategy
```javascript
// Same indexes on each monthly collection
// Slot_YYYYMM indexes
db.Slot_202605.createIndex({ "scheduleId": 1, "status": 1, "dateOnly": 1 })
db.Slot_202605.createIndex({ "status": 1, "dateOnly": 1 })
db.Slot_202605.createIndex({ "dateOnly": 1, "status": 1 })

// Appointment_YYYYMM indexes
db.Appointment_202605.createIndex({ "patientId": 1, "dateOnly": 1 })
db.Appointment_202605.createIndex({ "practitionerId": 1, "dateOnly": 1 })
db.Appointment_202605.createIndex({ "dateOnly": 1, "status": 1 })
db.Appointment_202605.createIndex({ "status": 1 })

// Apply same indexes to each new monthly collection automatically
```

#### Partition Lifecycle Management
```javascript
// Automatic partition creation (run monthly)
function createNextMonthPartitions() {
  const nextMonth = getNextMonthKey()  // e.g., "202606"
  
  // Create collections
  db.createCollection(`Slot_${nextMonth}`)
  db.createCollection(`Appointment_${nextMonth}`)
  db.createCollection(`AppointmentResponse_${nextMonth}`)
  
  // Create indexes
  createStandardIndexes(`Slot_${nextMonth}`)
  createStandardIndexes(`Appointment_${nextMonth}`)
  createStandardIndexes(`AppointmentResponse_${nextMonth}`)
}

// Archival process (run monthly for old data)
function archiveOldAppointments(cutoffMonths = 12) {
  const cutoffMonth = getMonthKey(today - cutoffMonths * 30)
  const oldCollections = db.getCollectionNames().filter(name => 
    name.startsWith('Appointment_') && name < `Appointment_${cutoffMonth}`
  )
  
  for (const collName of oldCollections) {
    // Move to historical archive
    db[collName].find({}).forEach(doc => {
      db.HistoricalAppointment.insertOne({
        ...doc,
        originalId: doc._id,
        archivedDate: new Date()
      })
    })
    
    // Drop old collection
    db[collName].drop()
  }
}
```

#### Benefits

1. **Unlimited Scalability**
   - Bounded collection size (max ~30-31 days per collection)
   - No document count limits per collection
   - New partitions created automatically
   - Linear growth over time

2. **Predictable Performance**
   - Query performance independent of total data volume
   - Each monthly collection has consistent size
   - Indexes remain small and efficient
   - No performance degradation with historical growth

3. **Efficient Archival**
   - Old months can be archived to cold storage
   - Drop entire collections instead of deleting documents
   - Easy to move to cheaper storage tiers
   - Historical data doesn't impact current performance

4. **Operational Flexibility**
   - Can apply different retention policies per resource type
   - Backup/restore operations on monthly granularity
   - Index rebuilds only affect one month
   - Testing and maintenance simplified

5. **Cost Optimization**
   - Hot data (current/future months) on fast storage
   - Warm data (3-6 months old) on standard storage
   - Cold data (>6 months) on archive storage
   - Pay only for storage tier you need

6. **Parallel Operations**
   - Different months can be processed in parallel
   - No lock contention across time periods
   - Reporting can aggregate months concurrently
   - Better resource utilization

#### Trade-offs

- **Application Complexity**: Routing logic required in application layer
- **Cross-Month Queries**: Slower for date ranges spanning multiple months
- **Index Multiplication**: Each partition needs full set of indexes
- **Management Overhead**: Must create/drop collections regularly
- **No Native Sharding**: MongoDB sharding is per-collection, not per-month
- **Cross-Partition Transactions**: Cannot use transactions across monthly collections

#### Best For
- **Very high-scale systems (millions of appointments/month)**
- **Long data retention requirements (5+ years)**
- **Systems with strict performance SLAs**
- **Multi-tenant platforms with time-based isolation**
- **Compliance requirements for data archival**
- **Cost-sensitive deployments with tiered storage**

#### Anti-Patterns to Avoid
❌ **Daily partitions**: Too granular, creates too many collections  
❌ **Yearly partitions**: Too coarse, loses performance benefits  
❌ **Mixed partitioning**: Don't partition Appointments but not Slots  
✅ **Monthly partitions**: Sweet spot for healthcare scheduling

---

### Strategy 5: Aggregate-First Materialized Views

#### Collection Structure
**Collections Required: 7-8**
1. `Schedule` - Canonical source
2. `Slot` - Canonical source
3. `Appointment` - Canonical source
4. `AppointmentResponse` - Canonical source
5. `AvailabilityView` - Materialized search view
6. `PatientAppointmentView` - Pre-computed patient views
7. `PractitionerScheduleView` - Pre-computed practitioner views
8. `ReportingView` - Aggregated reporting data

#### Data Model
```javascript
// Canonical Collections (Normalized FHIR)
// Schedule, Slot, Appointment - same as Strategy 1

// AvailabilityView - Materialized for Slot Search
{
  "_id": "avail-prac-123-2026-05-10",
  "practitionerId": "prac-123",
  "practitionerName": "Dr. John Smith",
  "specialty": "Cardiology",
  "date": "2026-05-10",
  "locationId": "loc-456",
  "locationName": "Main Hospital",
  
  "morningSlots": [
    { "slotId": "slot-001", "start": "09:00", "end": "09:30", "status": "free" },
    { "slotId": "slot-002", "start": "09:30", "end": "10:00", "status": "free" },
    { "slotId": "slot-003", "start": "10:00", "end": "10:30", "status": "busy" }
  ],
  "afternoonSlots": [ /* ... */ ],
  "eveningSlots": [ /* ... */ ],
  
  "summary": {
    "totalSlots": 24,
    "freeSlots": 18,
    "busySlots": 5,
    "unavailableSlots": 1,
    "utilizationRate": 0.25,
    "nextAvailableTime": "09:00"
  },
  
  "metadata": {
    "lastUpdated": "2026-05-09T14:23:00Z",
    "version": 5
  }
}

// PatientAppointmentView - Pre-computed for Patient Portal
{
  "_id": "patient-view-pat-123",
  "patientId": "pat-123",
  "patientName": "Jane Doe",
  
  "upcomingAppointments": [
    {
      "appointmentId": "appt-001",
      "date": "2026-05-10",
      "time": "09:00 AM",
      "practitioner": "Dr. John Smith",
      "specialty": "Cardiology",
      "location": "Main Hospital - Room 304",
      "status": "booked",
      "canReschedule": true,
      "canCancel": true,
      "virtualServiceUrl": null
    }
  ],
  
  "pastAppointments": [
    {
      "appointmentId": "appt-000",
      "date": "2026-04-15",
      "practitioner": "Dr. Smith",
      "status": "fulfilled"
    }
  ],
  
  "pendingRequests": [],
  "cancelledHistory": [],
  
  "statistics": {
    "totalAppointments": 12,
    "completed": 10,
    "cancelled": 1,
    "noShow": 1,
    "complianceRate": 0.83
  },
  
  "lastUpdated": "2026-05-09T14:23:00Z"
}

// PractitionerScheduleView - Daily Schedule
{
  "_id": "prac-view-prac-123-2026-05-10",
  "practitionerId": "prac-123",
  "practitionerName": "Dr. John Smith",
  "date": "2026-05-10",
  "dayOfWeek": "Friday",
  
  "schedule": [
    {
      "time": "09:00 - 09:30",
      "appointmentId": "appt-001",
      "patient": "Jane Doe",
      "patientId": "pat-123",
      "status": "booked",
      "type": "Follow-up",
      "location": "Room 304",
      "notes": "Check blood pressure"
    },
    {
      "time": "09:30 - 10:00",
      "status": "free"
    },
    {
      "time": "10:00 - 10:30",
      "appointmentId": "appt-002",
      "patient": "John Smith",
      "status": "booked",
      "type": "New Patient"
    }
  ],
  
  "summary": {
    "totalSlots": 16,
    "bookedAppointments": 8,
    "freeSlots": 7,
    "blockedSlots": 1,
    "utilizationRate": 0.50,
    "estimatedEndTime": "17:00"
  },
  
  "lastUpdated": "2026-05-09T14:23:00Z"
}

// ReportingView - Aggregated Metrics
{
  "_id": "report-daily-2026-05-10",
  "reportType": "daily",
  "date": "2026-05-10",
  
  "appointmentMetrics": {
    "totalScheduled": 245,
    "byStatus": {
      "booked": 200,
      "arrived": 15,
      "fulfilled": 20,
      "cancelled": 8,
      "noShow": 2
    },
    "bySpecialty": {
      "Cardiology": 45,
      "Orthopedics": 38,
      "GeneralPractice": 92,
      "Pediatrics": 70
    }
  },
  
  "capacityMetrics": {
    "totalSlots": 1200,
    "availableSlots": 320,
    "bookedSlots": 800,
    "unavailableSlots": 80,
    "utilizationRate": 0.67
  },
  
  "practitionerMetrics": {
    "totalPractitioners": 45,
    "activeToday": 42,
    "avgUtilization": 0.65,
    "topUtilization": [
      { "practitionerId": "prac-123", "name": "Dr. Smith", "rate": 0.95 },
      { "practitionerId": "prac-456", "name": "Dr. Jones", "rate": 0.89 }
    ]
  },
  
  "generatedAt": "2026-05-10T06:00:00Z"
}
```

#### View Update Strategy

**Option A: Change Streams (Real-time)**
```javascript
// Watch for changes and update views in real-time
const changeStream = db.Appointment.watch()

changeStream.on('change', async (change) => {
  if (change.operationType === 'insert' || change.operationType === 'update') {
    const appointment = change.fullDocument
    
    // Update PatientAppointmentView
    await updatePatientView(appointment.patientId)
    
    // Update PractitionerScheduleView
    await updatePractitionerView(appointment.practitionerId, appointment.dateOnly)
    
    // Update AvailabilityView if slot changed
    if (appointment.slot) {
      await updateAvailabilityView(appointment.slot)
    }
  }
})
```

**Option B: Scheduled Batch Jobs**
```javascript
// Rebuild views nightly at 2 AM
async function rebuildViews() {
  const today = new Date()
  const futureHorizon = new Date(today.getTime() + 90 * 24 * 60 * 60 * 1000)
  
  // Rebuild AvailabilityView for next 90 days
  await rebuildAvailabilityViews(today, futureHorizon)
  
  // Rebuild active patient views
  const activePatients = await getActivePatients(90)  // Active in last 90 days
  for (const patientId of activePatients) {
    await rebuildPatientView(patientId)
  }
  
  // Rebuild practitioner views for next 30 days
  await rebuildPractitionerViews(today, 30)
  
  // Generate daily reports
  await generateDailyReport(today)
}
```

**Option C: Lazy/On-Demand**
```javascript
// Update view when accessed if stale
async function getPatientAppointments(patientId) {
  const view = await db.PatientAppointmentView.findOne({ patientId })
  
  // Check if view is stale (older than 5 minutes)
  if (!view || (Date.now() - view.lastUpdated) > 5 * 60 * 1000) {
    // Rebuild view on-demand
    const rebuilt = await rebuildPatientView(patientId)
    return rebuilt
  }
  
  return view
}
```

#### Index Strategy
```javascript
// Canonical collections - standard indexes
// (same as Strategy 1)

// AvailabilityView indexes
db.AvailabilityView.createIndex({ "practitionerId": 1, "date": 1 })
db.AvailabilityView.createIndex({ 
  "specialty": 1, 
  "date": 1, 
  "summary.freeSlots": 1 
})
db.AvailabilityView.createIndex({ "locationId": 1, "date": 1 })
db.AvailabilityView.createIndex({ "date": 1 })

// PatientAppointmentView indexes
db.PatientAppointmentView.createIndex({ "patientId": 1 })
db.PatientAppointmentView.createIndex({ "lastUpdated": 1 })

// PractitionerScheduleView indexes
db.PractitionerScheduleView.createIndex({ "practitionerId": 1, "date": 1 })
db.PractitionerScheduleView.createIndex({ "date": 1 })

// ReportingView indexes
db.ReportingView.createIndex({ "reportType": 1, "date": 1 })
db.ReportingView.createIndex({ "date": 1 })
```

#### Benefits

1. **Extreme Read Performance**
   - Views pre-computed for common queries
   - Single document fetch for complex data
   - No aggregation pipelines at query time
   - Dashboard queries in milliseconds

2. **Complex Data Pre-processed**
   - Joins and aggregations done during view build
   - Application logic simplified
   - Frontend gets exactly what it needs
   - No client-side data transformation

3. **Flexible Update Strategies**
   - Can choose real-time (change streams)
   - Or batch updates (scheduled jobs)
   - Or lazy updates (on-demand)
   - Mix strategies for different views

4. **Canonical Data Preserved**
   - Source collections remain normalized
   - Full FHIR compliance maintained
   - Can rebuild views anytime from source
   - Audit and compliance not affected

5. **Optimized for Access Patterns**
   - Each view tailored to specific use case
   - Patient portal gets patient view
   - Practitioner app gets practitioner view
   - Reporting gets aggregated view
   - No one-size-fits-all compromise

6. **Reporting Performance**
   - Pre-aggregated metrics ready instantly
   - No expensive queries on operational data
   - Historical trends pre-computed
   - Business intelligence queries fast

#### Trade-offs

- **Storage Multiplier**: Views duplicate data (3-5x storage)
- **Update Latency**: Real-time updates have lag (eventual consistency)
- **View Staleness**: Batch updates mean stale data between runs
- **Build Time**: Initial view generation can take hours for large datasets
- **Complexity**: More collections, more update logic
- **Failure Handling**: View updates can fail, need retry logic

#### Best For
- **Patient/practitioner portal applications**
- **Analytics-heavy platforms**
- **Multi-tenant SaaS with per-customer views**
- **Systems with complex business logic**
- **High read:write ratios (50:1 or higher)**
- **Requirements for instant dashboard loads**

---

## Strategy Comparison Matrix

| Criteria | Normalized | Embedded Denorm | Hybrid | Time-Partitioned | Materialized Views |
|----------|-----------|----------------|--------|------------------|-------------------|
| **Read Performance** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Write Performance** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Storage Efficiency** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **Query Simplicity** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **FHIR Compliance** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Scalability** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Operational Complexity** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐ |
| **Consistency** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Data Freshness** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Cost** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |

**Legend**: ⭐ = Poor, ⭐⭐⭐ = Average, ⭐⭐⭐⭐⭐ = Excellent

---

## Recommended Strategy by Use Case

### Use Case 1: Small Healthcare Clinic (< 10K appointments/year)
**Recommendation**: **Strategy 1 (Normalized)**
- Simple to implement and maintain
- Storage cost negligible
- FHIR compliance straightforward
- Scalability not a concern

### Use Case 2: Mid-Size Hospital (10K - 100K appointments/year)
**Recommendation**: **Strategy 3 (Hybrid)**
- Balanced performance for growing load
- SlotIndex optimizes patient booking
- Still manageable operationally
- Room to scale

### Use Case 3: Large Hospital Network (100K - 1M appointments/year)
**Recommendation**: **Strategy 3 (Hybrid) + Strategy 4 (Time-Partitioning)**
- Hybrid for current/future appointments (3 months window)
- Time-partitioning for archival and historical data
- Combine strengths of both strategies
- Clear separation of hot and cold data

### Use Case 4: Regional Health System (1M+ appointments/year)
**Recommendation**: **Strategy 4 (Time-Partitioned) + Strategy 5 (Materialized Views)**
- Time-partitioning for operational scalability
- Materialized views for reporting and dashboards
- Multi-tier storage (hot/warm/cold)
- Optimized for both operational and analytical workloads

### Use Case 5: Patient Booking Platform (SaaS)
**Recommendation**: **Strategy 2 (Embedded) or Strategy 5 (Materialized Views)**
- Patient experience is primary concern
- Read latency must be minimal
- Can tolerate storage costs
- Update complexity managed by platform team

### Use Case 6: FHIR Server Implementation
**Recommendation**: **Strategy 1 (Normalized)**
- Must maintain strict FHIR compliance
- Interoperability is critical
- Standard FHIR search parameters
- Integration with other FHIR systems

---

## Implementation Recommendations

### Phase 1: Start Simple (Months 0-3)
1. Implement Strategy 1 (Normalized)
2. Instrument with performance monitoring
3. Identify actual query patterns
4. Measure read/write ratios
5. Document bottlenecks

### Phase 2: Optimize Hot Paths (Months 3-6)
1. Add selective denormalization (move toward Hybrid)
2. Create SlotIndex for availability search
3. Implement key indexes
4. Optimize top 10 slowest queries
5. Monitor storage growth

### Phase 3: Scale (Months 6-12)
1. Implement time-partitioning for historical data
2. Add materialized views for dashboards
3. Set up archival processes
4. Implement view rebuild automation
5. Plan for sharding if needed

### Phase 4: Optimize (Months 12+)
1. Fine-tune indexes based on usage patterns
2. Optimize view update frequencies
3. Implement tiered storage
4. Add caching layers
5. Continuous monitoring and adjustment

---

## Monitoring and Observability

### Key Metrics to Track

**Query Performance:**
- Average query latency by pattern (SP-1, SL-1, AP-1, etc.)
- 95th and 99th percentile latencies
- Slow query log analysis
- Index hit rates

**Write Performance:**
- Insert/update throughput
- Write amplification factor (denormalization overhead)
- View rebuild times
- Update lag (for materialized views)

**Storage:**
- Collection sizes over time
- Index sizes
- Storage growth rate
- Duplication factor

**Operational:**
- View staleness
- Partition creation/archival success rate
- Index rebuild times
- Query plan cache hit rates

### Alerting Thresholds

- 🔴 **Critical**: Query latency > 1 second for booking searches
- 🔴 **Critical**: View staleness > 15 minutes for patient views
- 🟡 **Warning**: Storage growth > 20% per month
- 🟡 **Warning**: Index size > 50% of collection size
- 🟢 **Info**: Partition created/archived successfully

---

## Conclusion

No single strategy fits all use cases. The optimal approach depends on:

1. **Scale**: Current and projected data volumes
2. **Access Patterns**: Read-heavy vs write-heavy
3. **Latency Requirements**: Real-time vs eventual consistency acceptable
4. **Budget**: Storage and compute cost constraints
5. **Team Expertise**: Operational complexity tolerance
6. **Compliance**: FHIR and healthcare regulatory requirements

**General Guidance:**
- **Start normalized** for simplicity and correctness
- **Add indexes** based on actual query patterns
- **Selectively denormalize** hot query paths
- **Partition** when data volume exceeds working set
- **Materialize views** for complex analytical queries

The best strategy is often a **hybrid of multiple approaches**, with different strategies for different resource types or time windows. Evolution over time is expected and healthy as requirements and scale change.

---

**Document Version**: 1.0  
**Last Updated**: May 6, 2026  
**Author**: FHIR Data Architecture Team  
**Next Review**: August 2026
