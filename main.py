from fastapi import FastAPI, HTTPException
from generator import generate_sentiment_report
from pydantic import BaseModel
from typing import Literal

app = FastAPI()

# Define valid categories
VALID_CATEGORIES = ["metals", "forex", "crypto", "index"]

class SentimentRequest(BaseModel):
    symbol: str
    category: Literal["metals", "forex", "crypto", "index"]

@app.post("/generate_sentiment")
async def generate_sentiment(data: SentimentRequest):
    try:
        if data.category not in VALID_CATEGORIES:
            raise HTTPException(status_code=400, detail="Invalid category")

        report = await generate_sentiment_report(data.symbol, data.category)
        return {
            "symbol": data.symbol,
            "category": data.category,
            "report": report
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
