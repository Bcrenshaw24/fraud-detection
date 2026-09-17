## Fraud Detection Pipeline

The services are connected in this order:

```text
producer -> Kafka -> Spark feature processor -> Postgres feature store -> API model
```

### Project layout

- `producer/` generates transaction events and publishes them to Kafka.
- `spark/` consumes events, builds rolling user features, and upserts them into Postgres.
- `api/` reads the latest features and serves `/predict`.
- `api/model/` contains the scoring model and its prediction interface. The current implementation is a baseline scorer and can be replaced with a trained model without changing the API route.
- `db/init.sql` creates the feature-store schema and table.

### Run locally

```bash
docker compose up --build
```

The prediction API is available at `http://localhost:8000`.
