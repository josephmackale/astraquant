from fastapi import FastAPI
from pydantic import BaseModel
from risk import RiskEngine

app = FastAPI()
risk_manager = RiskEngine()

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

@app.post("/assess")
async def assess(req: AssessRequest):
    acc = req.account.model_dump()
    result = risk_manager.assess_trade(acc)
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "service": "risk"}
