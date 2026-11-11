# Strategy Registry Endpoints

- `GET /strategies`
- `GET /strategies/{strategy_id}`
- `GET /strategies/{strategy_id}/spec`
- `GET /strategies/{strategy_id}/assets/{asset_path}`
- `GET /ops`
- `POST /ops`

Registry endpoints let clients discover strategy packs, inspect specs, and retrieve strategy assets.

## Portable deployment compilation

`POST /strategies/{strategy_id}/deployment-config/compile` compiles supplied configuration, analytics and dictionary snapshots into portable B-tree and Atlas Search definitions plus a projection contract. It supports the openEHR RPS Dual profiles without activation or target bindings. See `docs/deployment-configuration.md` for the request and release semantics.
