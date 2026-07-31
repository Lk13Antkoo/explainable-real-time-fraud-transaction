from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, create_model
import joblib
import numpy as np
import pandas as pd
import shap
# Load the trained model and scaler from disk
model = joblib.load("./output/xgb_model.pkl")
#scaler = joblib.load("./output/scaler.pkl")
explainer = shap.TreeExplainer(model)

# Create the FastAPI app
app = FastAPI(title="Fraud Detection API")

# Build the request schema from the processed training columns.
# This keeps the API aligned with the model input shape.
schema_sample = pd.read_csv("./data/processed/X_train.csv", nrows=2)
feature_columns = schema_sample.columns.tolist()
K = 5

def infer_python_type(series: pd.Series) -> type:
    if pd.api.types.is_bool_dtype(series):
        return bool
    if pd.api.types.is_integer_dtype(series):
        return int
    if pd.api.types.is_float_dtype(series):
        return float
    return str


Transaction = create_model(
    "Transaction",
    **{column: (infer_python_type(schema_sample[column]), ...) for column in feature_columns}
)

# Define what the response looks like
class ScoreResponse(BaseModel):
    risk_score: float
    is_flagged: bool
    message: str
    top_feature_names: list[str] = []

# The main endpoint
@app.post("/score")
def score_transaction(txn: dict):
    try:
        validated_txn = Transaction(**txn)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Convert transaction to a dataframe
    txn_data = validated_txn.model_dump() if hasattr(validated_txn, "model_dump") else validated_txn.dict()
    data = pd.DataFrame([txn_data])
    data = data[feature_columns]
    
    # Scale it using the same scaler from training
    #data_scaled = scaler.transform(data)
    
    # Get anomaly score from model
    raw_score = model.predict_proba(data)
    # Get SHAP values for explainability
    explain_values = explainer(data)
    values = explain_values.values[0]
    data = explain_values.data[0]
 
    # Top-K by |SHAP|
    idx_top = np.argsort(np.abs(values))[-K:]
    # sort ASC by |SHAP| → largest will be last (top)
    idx_top = idx_top[np.argsort(np.abs(values[idx_top]))]
    top_feature_names = [feature_columns[i] for i in idx_top]



    
    # Convert to 0-1 risk score (higher = more suspicious)
    risk = raw_score[0][1]  # Assuming the second column is the probability of being fraudulent
    
    # Flag if risk is above 0.7
    flagged = risk > 0.8
    
    # Create a message
    if flagged:
        msg = "SUSPICIOUS — This transaction has been flagged for review"
    else:
        msg = "NORMAL — This transaction looks legitimate"
    
    return ScoreResponse(
        risk_score=round(risk, 4),
        is_flagged=flagged,
        message=msg,
        top_feature_names=top_feature_names
    )

# A simple health check endpoint
@app.get("/")
def home():
    return {"status": "Fraud Detection API is running"}