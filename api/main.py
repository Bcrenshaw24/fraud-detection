import os 
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel 
import psycopg2 
from psycopg2.extras import RealDictCursor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from model.fraud_model import FraudModel

app = FastAPI(title="Real-Time Fraud Detection API")
fraud_model = FraudModel()
prediction_counter = Counter("fraud_predictions_total", "Total fraud predictions")
fraud_counter = Counter("fraud_predictions_flagged_total", "Predictions classified as fraud")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ml_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ml_password")

def get_db_connection(): 
    return psycopg2.connect( 
        host=POSTGRES_HOST, 
        database=POSTGRES_DB, 
        user=POSTGRES_USER, 
        password=POSTGRES_PASSWORD, 
        port=5432, 
        cursor_factory=RealDictCursor
    )

class TransactionRequest(BaseModel): 
    transaction_id: str
    user_id: str 
    amount: float 
    merchant_category: str 

@app.get("/health")
def health_check(): 
    return {"status": "healthy"}

@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/predict")
def predict_fraud(tx: TransactionRequest): 
    try: 
        conn = get_db_connection() 
        cursor = conn.cursor()

        query = """ 
                SELECT tx_count_5m, total_amount_5m, avg_amount_24h, unique_categories_1h
                FROM feature_store.user_features 
                WHERE user_id = %s;
        """
        cursor.execute(query, (tx.user_id,))
        features = cursor.fetchone() 

        cursor.close() 
        conn.close() 

        if not features: 
            tx_count_5m = 1 
            total_amount_5m = tx.amount
            avg_amount_24h = tx.amount 
        else: 
            tx_count_5m = features["tx_count_5m"]
            total_amount_5m = float(features["total_amount_5m"] or 0.0)
            avg_amount_24h = float(features["avg_amount_24h"] or 0.0)

        risk_score = fraud_model.predict_score(
            amount=tx.amount,
            tx_count_5m=tx_count_5m,
            avg_amount_24h=avg_amount_24h,
        )
        is_fraud = fraud_model.is_fraud(risk_score)
        prediction_counter.inc()
        if is_fraud:
            fraud_counter.inc()

        return { 
            "transaction_id": tx.transaction_id, 
            "user_id": tx.user_id, 
            "risk_score": round(risk_score, 2), 
            "is_fraud": is_fraud, 
            "features_used": { 
                "tx_count_5m": tx_count_5m, 
                "total_amount_5m": total_amount_5m, 
                "avg_amount_24h": avg_amount_24h
            }
        }
    except Exception as e: 
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        