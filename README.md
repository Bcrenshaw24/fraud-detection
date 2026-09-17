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

### Check the stack

```powershell
docker compose ps
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:9090/api/v1/targets
docker compose logs --tail=50 spark-processor
```

Send a test transaction to the model:

```powershell
$body = @{
	transaction_id = "test_001"
	user_id = "usr_0001"
	amount = 100.00
	merchant_category = "grocery"
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
	-Uri http://localhost:8000/predict `
	-ContentType "application/json" `
	-Body $body
```

Expected output includes `risk_score`, `is_fraud`, and `features_used`.
