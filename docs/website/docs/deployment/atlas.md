---
sidebar_position: 2
---

# MongoDB Atlas Setup

Configure MongoDB Atlas for \{kehrnel\} deployment.

## Prerequisites

- MongoDB Atlas account
- Cluster tier M10+ 
- Atlas Search enabled

## Cluster Setup

### 1. Create Cluster

1. Log in to [MongoDB Atlas](https://cloud.mongodb.com)
2. Create a new cluster:
   - **Provider**: AWS/GCP/Azure
   - **Region**: Choose closest to your users
   - **Tier**: M10+ for production workloads

### 2. Network Access

Add IP addresses or CIDR ranges:

1. Navigate to **Network Access**
2. Click **Add IP Address**
3. Add your application IP or use `0.0.0.0/0` for development

For production, use VPC Peering:

1. Navigate to **Network Access** → **Peering**
2. Configure peering with your VPC

### 3. Database User

Create a user for \{kehrnel\}:

1. Navigate to **Database Access**
2. Click **Add New Database User**
3. Configure:
   - **Authentication**: Password
   - **Built-in Role**: `readWriteAnyDatabase`

## Connection String

Get your connection string:

1. Click **Connect** on your cluster
2. Choose **Connect your application**
3. Copy the connection string

```bash
mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

Configure in \{kehrnel\}:

```bash
export CORE_MONGODB_URL="mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"
export CORE_DATABASE_NAME="kehrnel_db"
```

## Atlas Search Index

Create the required search index for cross-patient queries.

Recommended workflow:

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

kehrnel setup --runtime-url "$RUNTIME_URL" --env dev --domain openehr --strategy openehr.rps_dual
kehrnel strategy build-search-index --env dev --domain openehr --strategy openehr.rps_dual --out .kehrnel/search-index.json
```

### Using Atlas UI

1. Navigate to your cluster → **Search**
2. Click **Create Search Index**
3. Select **JSON Editor**
4. Choose collection: `compositions_search`
5. Index name: `search_nodes_index`
6. Paste the generated `.kehrnel/search-index.json`

### Using mongosh (optional)

```javascript
const definition = EJSON.parse(cat(".kehrnel/search-index.json"));
db.compositions_search.createSearchIndex({ name: "search_nodes_index", definition });
```

The generated definition reflects the active strategy config and mappings. In the current defaults it indexes root metadata such as `ehr_id`, `tid`, and `sort_time`, plus the mapped `sn.data.*` fields selected for analytics.

## Standard Indexes

Create B-tree indexes for patient-scoped queries:

```javascript
// Patient-scoped path predicates
db.compositions_rps.createIndex({ "ehr_id": 1, "cn.p": 1, "time_c": 1 })

// Search collection paging/order helper
db.compositions_search.createIndex({ "ehr_id": 1, "sort_time": 1 })

// EHR collection
db.ehr.createIndex({ "ehr_id": 1 }, { unique: true })

// Contributions
db.contributions.createIndex({ "ehr_id": 1, "time_committed": -1 })
```

Activation creates missing indexes but does not drop obsolete ones. If you
previously provisioned `compositions_rps` with older Kehrnel examples, remove
the superseded base indexes manually:

```javascript
db.compositions_rps.dropIndex("ehr_id_1_v_1")
db.compositions_rps.dropIndex("ehr_id_1_tid_1_time_c_1_comp_id_1")
```

## Performance Optimization

### Cluster Sizing

| Workload | Recommended Tier | Storage |
|----------|------------------|---------|
| Development | M10 | 10GB |
| Small production | M30 | 100GB |
| Medium production | M50 | 500GB |
| Large production | M80+ | 1TB+ |

### Auto-Scaling

Enable auto-scaling for variable workloads:

1. Navigate to **Cluster** → **Configuration**
2. Enable **Auto-scale compute**
3. Set min/max cluster tiers

### Connection Pooling

Configure connection pooling in your connection string:

```
mongodb+srv://<username>:<password>@cluster.mongodb.net/?maxPoolSize=100&minPoolSize=10
```

## Backup and Recovery

### Continuous Backup

1. Navigate to **Backup**
2. Enable **Continuous Backup**
3. Configure retention policy

### Point-in-Time Recovery

For M10+ clusters:

1. Navigate to **Backup** → **Restore**
2. Choose **Point in Time**
3. Select timestamp

## Monitoring

### Atlas Metrics

Monitor via Atlas dashboard:

- Operations per second
- Document reads/writes
- Index hits
- Query targeting

### Alerts

Configure alerts:

1. Navigate to **Alerts**
2. Create rules for:
   - High CPU usage
   - High memory usage
   - Slow queries
   - Replication lag

## Security

### TLS/SSL

Atlas connections are encrypted by default. For additional security:

```bash
export KEHRNEL_MONGO_TLS_ALLOW_INVALID_CERTS=false
export KEHRNEL_MONGO_TLS_CA_FILE=/path/to/ca.pem
```

### IP Whitelist

For production, use IP whitelisting instead of `0.0.0.0/0`:

1. Add specific IP ranges
2. Use VPC Peering for internal access

### Audit Logging

Enable audit logs for compliance:

1. Navigate to **Security** → **Advanced**
2. Enable **Database Auditing**

## Environment Configuration

### Development

```bash
CORE_MONGODB_URL=mongodb+srv://<username>:<password>@dev-cluster.mongodb.net
CORE_DATABASE_NAME=kehrnel_dev
KEHRNEL_AUTH_ENABLED=false
```

### Staging

```bash
CORE_MONGODB_URL=mongodb+srv://<username>:<password>@staging-cluster.mongodb.net
CORE_DATABASE_NAME=kehrnel_staging
KEHRNEL_AUTH_ENABLED=true
KEHRNEL_API_KEYS=<staging-api-key>
```

### Production

```bash
CORE_MONGODB_URL=mongodb+srv://<username>:<password>@prod-cluster.mongodb.net
CORE_DATABASE_NAME=kehrnel_prod
KEHRNEL_AUTH_ENABLED=true
KEHRNEL_API_KEYS=<prod-api-key-1>,<prod-api-key-2>
KEHRNEL_RATE_LIMIT=120
```

## Related

- [Docker Deployment](/docs/deployment/docker) - Container setup
- [Production Guide](/docs/deployment/production) - Best practices
- [Configuration](/docs/getting-started/configuration) - Environment variables
