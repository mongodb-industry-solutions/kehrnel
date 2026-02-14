# Strategy Synthetic And Job Endpoints

## Strategy synthetic

- `POST /api/strategies/openehr/rps_dual/synthetic/generate`
- `GET /api/strategies/openehr/rps_dual/synthetic/stats`

## Environment-scoped synthetic jobs

- `GET /environments/{env_id}/synthetic/jobs`
- `POST /environments/{env_id}/synthetic/jobs`
- `GET /environments/{env_id}/synthetic/jobs/{job_id}`
- `POST /environments/{env_id}/synthetic/jobs/{job_id}/cancel`

These APIs power sampling and synthetic data generation workflows.
