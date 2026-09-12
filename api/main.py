import os 
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel 
import psycopg2 
from psycopg2.extras import RealDictCursor

app = FastAPI(titel="Real-Time Fraud Derection API")

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
            total_amount_24h = tx.amount 
            avg_amount_24h = tx.amount 
        else: 
            tx_count_5m = features["tx_count_5m"]
            total_amount_5m = float(features["total_amount_5m"] or 0.0)
            avg_amount_24h = float(features["avg_amount_24h"] or 0.0)

        #Filler heuristic, replace with XGBoost/RF
        risk_score = 0.05 

        if tx_count_5m > 5: 
            risk_score += 0.40 
        if tx.amount > (avg_amount_24h * 3.0) and avg_amount_24h > 0: 
            risk_score += 0.45 

        is_fraud = risk_score >= .70 

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
        