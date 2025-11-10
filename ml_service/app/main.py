from fastapi import FastAPI, HTTPException  # Добавлен HTTPException
from pydantic import BaseModel
import logging
from typing import Dict, Any
from transformers import pipeline

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Sentiment Analysis API",
    description="API для анализа тональности текста о чипсах",
    version="1.0.0"
)

class PredictionRequest(BaseModel):
    text: str

class PredictionResponse(BaseModel):
    text: str
    sentiment: str
    confidence: float
    model_used: str

class ModelManager:
    def __init__(self):
        self.model = None
        self.model_loaded = False  # ДОБАВЛЕНО
        self.load_model()
    
    def load_model(self):
        try:
            logger.info("🤖 Загружаем трансформер модель...")
            self.model = pipeline(
                'text-classification',
                model='./models/best_model_transformer',
                tokenizer='./models/best_model_tokenaiser'
            )
            self.model_loaded = True  # ДОБАВЛЕНО
            logger.info("✅ Трансформер модель загружена")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки модели: {e}")
            self.model_loaded = False  # ДОБАВЛЕНО
    
    def predict(self, text: str) -> Dict[str, Any]:
        if not self.model:
            raise Exception("Модель не загружена")
        
        result = self.model(text)[0]
        
        return {
            "sentiment": result["label"],
            "confidence": result["score"],
            "model_used": "transformer"
        }

# Инициализация менеджера моделей
model_manager = ModelManager()

@app.get("/")
async def root():
    return {
        "message": "Sentiment Analysis API", 
        "status": "running",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy" if model_manager.model_loaded else "loading",
        "model_loaded": model_manager.model_loaded
    }

@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    try:
        logger.info(f"📝 Получен текст для анализа: {request.text}")
        
        result = model_manager.predict(request.text)
        
        return PredictionResponse(
            text=request.text,
            sentiment=result["sentiment"],
            confidence=result["confidence"],
            model_used=result["model_used"]
        )
    except Exception as e:
        logger.error(f"❌ Ошибка предсказания: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/models")
async def list_models():
    return {
        "available_models": ["transformer"],
        "default_model": "transformer"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)