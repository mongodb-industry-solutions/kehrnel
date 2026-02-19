# Strategy Synthetic And Job Endpoints

## OpenAPI Docs

- Swagger (strategy): `/docs/strategies/openehr/rps_dual`
- ReDoc (strategy): `/redoc/strategies/openehr/rps_dual`
- Swagger (core jobs): `/docs/core`
- ReDoc (core jobs): `/redoc/core`

## Strategy synthetic

- `POST /api/strategies/openehr/rps_dual/synthetic/generate`
- `GET /api/strategies/openehr/rps_dual/synthetic/stats`

## Environment-scoped synthetic jobs

- `GET /environments/{env_id}/synthetic/jobs`
- `POST /environments/{env_id}/synthetic/jobs`
- `GET /environments/{env_id}/synthetic/jobs/{job_id}`
- `POST /environments/{env_id}/synthetic/jobs/{job_id}/cancel`
