---
sidebar_position: 3
---

# Production Deployment

Best practices for deploying \{kehrnel\} in production environments.

## Security Checklist

### Authentication

- [ ] Enable API key authentication
- [ ] Use strong, unique API keys
- [ ] Separate admin and regular API keys
- [ ] Implement key rotation policy

```bash
# Enable authentication
KEHRNEL_AUTH_ENABLED=true

# Regular API keys
KEHRNEL_API_KEYS=key1,key2,key3

# Admin API keys (for activation/management)
KEHRNEL_ADMIN_API_KEYS=admin-key-1
```

### Network Security

- [ ] Use HTTPS/TLS termination
- [ ] Configure firewall rules
- [ ] Use private networks for MongoDB
- [ ] Enable CORS restrictions

```bash
# Restrict CORS origins
KEHRNEL_CORS_ORIGINS=https://app.example.com,https://admin.example.com
```

### MongoDB Security

- [ ] Use TLS for MongoDB connections
- [ ] Use strong database passwords
- [ ] Enable MongoDB authentication
- [ ] Configure IP whitelisting

```bash
CORE_MONGODB_URL=mongodb+srv://<username>:<strong-password>@cluster.mongodb.net
KEHRNEL_MONGO_TLS_ALLOW_INVALID_CERTS=false
```

## Performance Configuration

### Rate Limiting

```bash
# Requests per minute per client
KEHRNEL_RATE_LIMIT=120

# Maximum tracked clients
KEHRNEL_RATE_LIMIT_MAX_CLIENTS=10000
```

### Query Limits

```bash
# Maximum rows per query
KEHRNEL_MAX_QUERY_RESULTS=5000

# Query timeout (ms)
KEHRNEL_MAX_QUERY_TIME_MS=30000

# Stored query limit
KEHRNEL_MAX_STORED_QUERY_LIST=500
```

### File Handling

```bash
# Maximum upload size (bytes)
KEHRNEL_MAX_UPLOAD_BYTES=10485760

# Maximum OPT template size
KEHRNEL_MAX_OPT_BYTES=5242880
```

## High Availability

### Load Balancing

Deploy multiple \{kehrnel\} instances behind a load balancer:

```yaml
# docker-compose.yml
services:
  kehrnel:
    deploy:
      replicas: 3
      update_config:
        parallelism: 1
        delay: 10s
      restart_policy:
        condition: on-failure
```

### Health Checks

Configure health check endpoints:

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

### Database High Availability

Use MongoDB replica sets or Atlas with:

- Minimum 3-node replica set
- Automated failover
- Read preference configuration

```bash
CORE_MONGODB_URL=mongodb+srv://...?retryWrites=true&w=majority&readPreference=secondaryPreferred
```

## Monitoring

### Application Metrics

Monitor these key metrics:

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Response time (P95) | &lt;100ms | &gt;500ms |
| Error rate | &lt;0.1% | &gt;1% |
| Request rate | - | Spike detection |
| Active connections | - | &gt;80% capacity |

### Database Metrics

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Query time (P95) | &lt;50ms | &gt;200ms |
| Index hit ratio | &gt;99% | &lt;95% |
| Connection pool | &lt;80% | &gt;90% |
| Replication lag | &lt;1s | &gt;10s |

### Logging

Configure structured logging:

```bash
KEHRNEL_DEBUG=false
```

Log aggregation recommendations:
- Use JSON log format
- Send to centralized logging (ELK, CloudWatch, etc.)
- Retain logs for compliance requirements

## Backup Strategy

### Database Backups

1. **Continuous backup** via Atlas
2. **Daily snapshots** for point-in-time recovery
3. **Off-site copies** for disaster recovery

### Configuration Backups

Back up:
- Environment configuration
- Strategy pack configurations
- Activation registry
- Code dictionaries

```bash
# Export activation registry
mongodump --uri="$CORE_MONGODB_URL" --db="$CORE_DATABASE_NAME" \
  --collection="_kehrnel_activations" --out=./backup

# Export code dictionaries
mongodump --uri="$CORE_MONGODB_URL" --db="$CORE_DATABASE_NAME" \
  --collection="_codes" --out=./backup
```

## Environment Isolation

### Multi-Environment Setup

```
┌─────────────────────────────────────────────────────────┐
│                     Production                          │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────┐ │
│  │   Kehrnel     │  │   Kehrnel     │  │  Kehrnel    │ │
│  │  Instance 1   │  │  Instance 2   │  │ Instance 3  │ │
│  └───────────────┘  └───────────────┘  └─────────────┘ │
│              │              │              │            │
│              └──────────────┼──────────────┘            │
│                             ▼                           │
│                ┌───────────────────────┐                │
│                │    MongoDB Atlas      │                │
│                │    (Prod Cluster)     │                │
│                └───────────────────────┘                │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                      Staging                            │
│  ┌───────────────────────────────────────────────────┐ │
│  │              Kehrnel Instance                      │ │
│  └───────────────────────────────────────────────────┘ │
│                         │                               │
│                         ▼                               │
│           ┌───────────────────────────┐                 │
│           │    MongoDB Atlas          │                 │
│           │    (Staging Cluster)      │                 │
│           └───────────────────────────┘                 │
└─────────────────────────────────────────────────────────┘
```

### API Key Scoping

Restrict API keys to specific environments:

```bash
KEHRNEL_API_KEY_ENV_SCOPES='{"prod-key": ["production"], "staging-key": ["staging"], "admin-key": "*"}'
```

## Scaling Guidelines

### Vertical Scaling

| Users | Recommended | Memory | CPU |
|-------|-------------|--------|-----|
| &lt;1000 | 1 instance | 2GB | 2 cores |
| 1000-10000 | 2-3 instances | 4GB each | 4 cores each |
| 10000-100000 | 3-5 instances | 8GB each | 8 cores each |
| &gt;100000 | 5+ instances | 16GB each | 16 cores each |

### Horizontal Scaling

Add instances when:
- CPU usage consistently &gt;70%
- Response time P95 &gt;100ms
- Connection pool &gt;80% utilized

## Deployment Procedure

### Rolling Update

```bash
# Update one instance at a time
docker service update --image kehrnel:v2.0.0 \
  --update-parallelism 1 \
  --update-delay 30s \
  kehrnel_api
```

### Blue-Green Deployment

1. Deploy new version to "green" environment
2. Run health checks
3. Switch load balancer to green
4. Monitor for issues
5. Decommission "blue" environment

### Rollback Procedure

```bash
# Quick rollback
docker service update --rollback kehrnel_api

# Or deploy previous version
docker service update --image kehrnel:v1.9.0 kehrnel_api
```

## Compliance Considerations

### Healthcare Compliance

For healthcare deployments:

- [ ] Enable audit logging
- [ ] Configure data retention policies
- [ ] Implement access controls
- [ ] Document security measures
- [ ] Regular security assessments

### Data Retention

Configure MongoDB TTL indexes if needed:

```javascript
// Auto-delete audit logs after 7 years
db.audit_logs.createIndex(
  { "timestamp": 1 },
  { expireAfterSeconds: 220752000 }
)
```

## Related

- [Docker Deployment](/docs/deployment/docker) - Container setup
- [Atlas Setup](/docs/deployment/atlas) - MongoDB configuration
- [Configuration](/docs/getting-started/configuration) - All environment variables
