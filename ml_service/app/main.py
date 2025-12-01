from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
import os
from typing import Dict, Any
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer

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
        self.model_loaded = False
        
    def load_model(self):
        try:
            logger.info("🤖 Загружаем трансформер модель...")
            
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
            model_path = os.path.join(base_dir, 'models', 'best_model_transformer')
            
            logger.info(f"📁 Путь к модели: {model_path}")
            
            if not os.path.exists(model_path):
                logger.error(f"❌ Модель не найдена по пути: {model_path}")
                models_dir = os.path.join(base_dir, 'models')
                if os.path.exists(models_dir):
                    logger.info(f"📂 Содержимое папки models: {os.listdir(models_dir)}")
                return
            
            required_files = ['config.json', 'model.safetensors', 'tokenizer.json']
            for file in required_files:
                file_path = os.path.join(model_path, file)
                if not os.path.exists(file_path):
                    logger.error(f"❌ Не найден файл: {file}")
                    logger.info(f"📂 Содержимое папки модели: {os.listdir(model_path)}")
                    return
            
            logger.info("✅ Все необходимые файлы найдены")
            
            self.model = pipeline(
                'text-classification',
                model=model_path, 
                device=-1  # CPU
            )
            
            self.model_loaded = True
            logger.info("✅ Трансформер модель успешно загружена")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки модели: {e}")
            import traceback
            logger.error(f"🔍 Детали ошибки: {traceback.format_exc()}")
            self.model_loaded = False
    
    def predict(self, text: str) -> Dict[str, Any]:
        if not self.model or not self.model_loaded:
            raise Exception("Модель не загружена")
        
        result = self.model(text)[0]
        
        return {
            "sentiment": result["label"],
            "confidence": result["score"],
            "model_used": "transformer"
        }

model_manager = ModelManager()

@app.on_event("startup")
async def startup_event():
    """Загружаем модель при старте приложения"""
    model_manager.load_model()

@app.get("/")
async def root():
    return {
        "message": "Sentiment Analysis API", 
        "status": "running",
        "model_loaded": model_manager.model_loaded,
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
        if not model_manager.model_loaded:
            raise HTTPException(status_code=503, detail="Модель еще не загружена")
            
        logger.info(f"📝 Получен текст для анализа: {request.text}")
        
        result = model_manager.predict(request.text)
        
        logger.info(f"✅ Предсказание успешно: {result}")
        
        return PredictionResponse(
            text=request.text,
            sentiment=result["sentiment"],
            confidence=result["confidence"],
            model_used=result["model_used"]
        )
    except Exception as e:
        logger.error(f"❌ Ошибка предсказания: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/paths")
async def debug_paths():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(base_dir, 'models', 'best_model_transformer')
    
    models_dir = os.path.join(base_dir, 'models')
    dir_contents = []
    if os.path.exists(models_dir):
        dir_contents = os.listdir(models_dir)
    
    model_dir_contents = []
    if os.path.exists(model_path):
        model_dir_contents = os.listdir(model_path)
    
    return {
        "base_directory": base_dir,
        "model_path": model_path,
        "model_exists": os.path.exists(model_path),
        "models_directory_contents": dir_contents,
        "model_directory_contents": model_dir_contents
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)