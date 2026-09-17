class FraudModel:
    """Baseline fraud scorer with a stable interface for a trained model."""

    fraud_threshold = 0.70

    def predict_score(
        self,
        *,
        amount: float,
        tx_count_5m: int,
        avg_amount_24h: float,
    ) -> float:
        risk_score = 0.05

        if tx_count_5m > 5:
            risk_score += 0.40
        if avg_amount_24h > 0 and amount > avg_amount_24h * 3.0:
            risk_score += 0.45

        return round(min(risk_score, 1.0), 2)

    def is_fraud(self, risk_score: float) -> bool:
        return risk_score >= self.fraud_threshold