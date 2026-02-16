---
sidebar_position: 1
---

# Docker Deployment

Deploy \{kehrnel\} using Docker containers.

## Quick Start

### Docker Compose

Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  kehrnel:
    image: kehrnel/kehrnel:latest
    ports:
      - "8000:8000"
    environment:
      - CORE_MONGODB_URL=mongodb://mongo:27017
      - CORE_DATABASE_NAME=kehrnel_db
      - KEHRNEL_API_HOST=0.0.0.0
      - KEHRNEL_API_PORT=8000
      - KEHRNEL_AUTH_ENABLED=false
    depends_on:
      - mongo

  mongo:
    image: mongo:6.0
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db

volumes:
  mongodb_data:
```

Start the services:

```bash
docker-compose up -d
```

## Building the Image

### Dockerfile

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy application
COPY src/ src/

# Create non-root user
RUN useradd -m kehrnel
USER kehrnel

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run application
CMD ["kehrnel-api", "--host", "0.0.0.0", "--port", "8000"]
```

Build the image:

```bash
docker build -t kehrnel:local .
```

## Configuration

### Environment Variables

```bash
docker run -d \
  --name kehrnel \
  -p 8000:8000 \
  -e CORE_MONGODB_URL="mongodb://host.docker.internal:27017" \
  -e CORE_DATABASE_NAME="kehrnel_db" \
  -e KEHRNEL_AUTH_ENABLED="true" \
  -e KEHRNEL_API_KEYS="key1,key2" \
  kehrnel:local
```

### Using .env File

```bash
docker run -d \
  --name kehrnel \
  -p 8000:8000 \
  --env-file .env.local \
  kehrnel:local
```

### Secrets Management

For sensitive values, use Docker secrets:

```yaml
version: '3.8'

services:
  kehrnel:
    image: kehrnel/kehrnel:latest
    secrets:
      - mongodb_url
      - api_keys
    environment:
      - CORE_MONGODB_URL_FILE=/run/secrets/mongodb_url
      - KEHRNEL_API_KEYS_FILE=/run/secrets/api_keys

secrets:
  mongodb_url:
    file: ./secrets/mongodb_url.txt
  api_keys:
    file: ./secrets/api_keys.txt
```

## Production Compose

```yaml
version: '3.8'

services:
  kehrnel:
    image: kehrnel/kehrnel:latest
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    ports:
      - "8000:8000"
    environment:
      - CORE_MONGODB_URL=${MONGODB_URL}
      - CORE_DATABASE_NAME=${DATABASE_NAME}
      - KEHRNEL_AUTH_ENABLED=true
      - KEHRNEL_API_KEYS=${API_KEYS}
      - KEHRNEL_RATE_LIMIT=120
      - KEHRNEL_MAX_QUERY_RESULTS=5000
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "3"

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/nginx/certs:ro
    depends_on:
      - kehrnel
```

## Networking

### Internal Network

```yaml
services:
  kehrnel:
    networks:
      - backend
      - frontend

  mongo:
    networks:
      - backend

networks:
  backend:
    internal: true
  frontend:
```

### With Load Balancer

```yaml
services:
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro

  kehrnel:
    image: kehrnel/kehrnel:latest
    deploy:
      replicas: 3
    expose:
      - "8000"
```

nginx.conf:
```nginx
upstream kehrnel {
    server kehrnel:8000;
}

server {
    listen 80;

    location / {
        proxy_pass http://kehrnel;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Volumes

### Persistent Data

```yaml
volumes:
  kehrnel_bundles:
    driver: local
  kehrnel_registry:
    driver: local

services:
  kehrnel:
    volumes:
      - kehrnel_bundles:/app/.kehrnel/bundles
      - kehrnel_registry:/app/.kehrnel_registry.json
```

## Health Checks

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' kehrnel

# View health check logs
docker inspect --format='{{json .State.Health}}' kehrnel | jq
```

## Logging

### View Logs

```bash
# Follow logs
docker logs -f kehrnel

# Last 100 lines
docker logs --tail 100 kehrnel

# With timestamps
docker logs -t kehrnel
```

### Log to File

```yaml
services:
  kehrnel:
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "5"
```

## Related

- [Atlas Deployment](/docs/deployment/atlas) - MongoDB Atlas setup
- [Production Guide](/docs/deployment/production) - Production best practices
