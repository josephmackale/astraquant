from fastapi import FastAPI
from pydantic import BaseModel
from risk import RiskEngine

app = FastAPI(
    title="AstraQuant Risk Service",
    version="1.0.0",
    description="Risk management microservice for AstraQuant"
)

risk_engine = RiskEngine(debug=False)


# -----------------------------
# MODELS
# -----------------------------
class Account(BaseModel):
    account_id: str
    balance: float
    recent_winrate: float
    current_minute_exposure: float
    volatility: float
    mode: str
    arc_mode: bool
    session_profit: float | None = None
    target: float | None = None
    surplus_mode: bool | None = None
    confidence: float | None = None
    recent_performance_factor: float | None = 1.0


class AssessRequest(BaseModel):
    account: Account


# -----------------------------
# ENDPOINTS
# -----------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/assess")
async def assess(req: AssessRequest):
    acc = req.account.model_dump()
    result = risk_engine.assess_trade(acc)
    return result
