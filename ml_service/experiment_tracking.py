import mlflow
import mlflow.sklearn
import mlflow.keras
import mlflow.transformers
from sklearn.metrics import f1_score, accuracy_score
import numpy as np

class ExperimentTracker:
    def __init__(self, experiment_name="SentimentAnalysis"):
        mlflow.set_experiment(experiment_name)
    
    def log_sklearn_experiment(self, model, vectorizer, encoder, X_test, y_test, params):
        with mlflow.start_run(run_name="MultinomialNB"):
            # Предсказания и метрики
            y_pred = model.predict(X_test)
            f1 = f1_score(y_test, y_pred, average='weighted')
            accuracy = accuracy_score(y_test, y_pred)
            
            # Логируем параметры
            mlflow.log_params(params)
            mlflow.log_metrics({"f1_score": f1, "accuracy": accuracy})
            
            # Логируем модель и артефакты
            mlflow.sklearn.log_model(model, "model")
            mlflow.sklearn.log_model(vectorizer, "vectorizer")
            mlflow.sklearn.log_model(encoder, "encoder")
            
            print(f"✅ MultinomialNB logged: F1={f1:.4f}")
    
    def log_lstm_experiment(self, model, tokenizer, encoder, X_test, y_test, params):
        with mlflow.start_run(run_name="LSTM"):
            # Предсказания
            y_pred_proba = model.predict(X_test)
            y_pred = np.argmax(y_pred_proba, axis=1)
            
            f1 = f1_score(y_test, y_pred, average='weighted')
            accuracy = accuracy_score(y_test, y_pred)
            
            mlflow.log_params(params)
            mlflow.log_metrics({"f1_score": f1, "accuracy": accuracy})
            
            # Логируем Keras модель
            mlflow.keras.log_model(model, "model")
            mlflow.sklearn.log_model(tokenizer, "tokenizer")
            mlflow.sklearn.log_model(encoder, "encoder")
            
            print(f"✅ LSTM logged: F1={f1:.4f}")
    
    def log_transformer_experiment(self, trainer, tokenizer, encoder, dataset_test, params):
        with mlflow.start_run(run_name="Transformer"):
            # Оценка модели
            eval_results = trainer.evaluate(dataset_test)
            
            mlflow.log_params(params)
            mlflow.log_metrics({
                "f1_score": eval_results['eval_f1-score'],
                "accuracy": eval_results['eval_accuracy'],
                "eval_loss": eval_results['eval_loss']
            })
            
            # Логируем трансформер
            mlflow.transformers.log_model(
                transformers_model={
                    'model': trainer.model,
                    'tokenizer': tokenizer
                },
                artifact_path="model",
                registered_model_name="sentiment-transformer"
            )
            mlflow.sklearn.log_model(encoder, "encoder")
            
            print(f"✅ Transformer logged: F1={eval_results['eval_f1-score']:.4f}")