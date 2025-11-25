import streamlit as st
from transformers import pipeline
import os
from sqlalchemy import create_engine, text

# Получаем абсолютный путь к текущей директории
current_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(current_dir, 'best_model_transformer_fixed')
tokenizer_path = os.path.join(current_dir, 'best_model_tokenaiser_fixed')

st.set_page_config(page_title="Text Classification", layout="wide")
st.title("🎯 Классификатор отзывов о чипсах")
st.markdown("---")

engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5433/airflow")

@st.cache_resource
def load_model():
    try:
        classifier = pipeline(
            'text-classification',
            model=model_path,
            tokenizer=tokenizer_path,
            device=-1
        )
        return classifier
    except Exception as e:
        st.error(f"Ошибка: {e}")
        return None

model = load_model()

# Все возможные метки с эмодзи
LABELS = {
    "ВКУС_POSITIVE": "😋 Положительный вкус",
    "ВКУС_NEGATIVE": "😖 Отрицательный вкус", 
    "ВКУС_NEUTRAL": "😐 Нейтральный вкус",
    "ТЕКСТУРА_POSITIVE": "👍 Положительная текстура",
    "ТЕКСТУРА_NEGATIVE": "👎 Отрицательная текстура",
    "ТЕКСТУРА_NEUTRAL": "✋ Нейтральная текстура",
    "ПАЧКА_POSITIVE": "📦 Положительная упаковка",
    "ПАЧКА_NEGATIVE": "📦 Отрицательная упаковка", 
    "ПАЧКА_NEUTRAL": "📦 Нейтральная упаковка",
    "O": "🔹 Нет аспекта"
}

# Инициализируем session state для хранения результата
if 'last_result' not in st.session_state:
    st.session_state.last_result = None
if 'last_text' not in st.session_state:
    st.session_state.last_text = ""
if 'show_thankyou' not in st.session_state:
    st.session_state.show_thankyou = False
if 'original_label' not in st.session_state:
    st.session_state.original_label = ""

if model is not None:
    st.success("✅ Модель успешно загружена!")
    
    # Показываем благодарность если нужно
    if st.session_state.show_thankyou:
        st.success("🎉 **Спасибо тебе большое! Ты помогаешь улучшать нашу модель!** 🚀")
        st.balloons()
        # Автоматически скрываем через 5 секунд или при следующем действии
        if st.button("Продолжить классификацию"):
            st.session_state.show_thankyou = False
            st.rerun()
    
    # Основной блок классификации
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🔍 Анализ текста")
        text_input = st.text_area(
            "Введите текст отзыва о чипсах:",
            height=150,
            placeholder="Например: Чипсы очень хрустящие и вкусные, но упаковка легко рвется...",
            value=st.session_state.last_text
        )
        
        if st.button("🎯 Классифицировать", type="primary", use_container_width=True):
            if not text_input.strip():
                st.warning("⚠️ Пожалуйста, введите текст для анализа")
            else:
                with st.spinner("🔮 Анализируем текст..."):
                    try:
                        result = model(text_input)[0]
                        st.session_state.last_result = result
                        st.session_state.last_text = text_input
                        st.session_state.original_label = result['label']
                        st.session_state.show_thankyou = False  # Сбрасываем благодарность
                        st.rerun()  # Перезагружаем для отображения результата
                        
                    except Exception as e:
                        st.error(f"❌ Ошибка при классификации: {e}")
    
    with col2:
        st.subheader("📚 Справка по меткам")
        st.markdown("""
        **Аспекты качества:**
        - 😋 **Вкус** - вкусовые характеристики
        - 👍 **Текстура** - хруст, структура  
        - 📦 **Упаковка** - внешний вид, целостность
        - 🔹 **Нет аспекта** - общие высказывания
        """)
        
        # Быстрый выбор примеров
        st.subheader("🚀 Быстрые примеры")
        examples = {
            "Вкусные и хрустящие": "ВКУС_POSITIVE",
            "Слишком соленые": "ВКУС_NEGATIVE", 
            "Мягкие и не хрустят": "ТЕКСТУРА_NEGATIVE",
            "Красивая упаковка": "ПАЧКА_POSITIVE"
        }
        
        for example_text, example_label in examples.items():
            if st.button(f"» {example_text}", key=example_text):
                st.session_state.last_text = example_text
                st.session_state.show_thankyou = False  # Сбрасываем благодарность
                st.rerun()
    
    # Отображаем результат если есть
    if st.session_state.last_result and not st.session_state.show_thankyou:
        result = st.session_state.last_result
        text_input = st.session_state.last_text
        
        # Красивое отображение результата
        st.success("✅ Анализ завершен!")
        
        # Карточка с результатом
        with st.container():
            st.markdown("### 📊 Результат классификации")
            col_res1, col_res2 = st.columns(2)
            
            with col_res1:
                label_display = LABELS.get(result['label'], result['label'])
                st.metric(
                    label="**Предсказанная метка**",
                    value=label_display
                )
            
            with col_res2:
                confidence_percent = result['score'] * 100
                st.metric(
                    label="**Уверенность модели**", 
                    value=f"{confidence_percent:.1f}%"
                )
        
        # Визуализация уверенности
        st.progress(float(result['score']))
        st.caption(f"Уверенность: {result['score']:.4f}")
        
        # Блок коррекции и сохранения
        st.markdown("---")
        st.subheader("✏️ Корректировка результата")
        
        col_corr1, col_corr2 = st.columns([3, 1])
        
        with col_corr1:
            st.info("Если модель ошиблась, выберите правильную метку:")
            
            # Все метки в одном радио-списке
            correct_label = st.radio(
                "**Выберите правильную метку:**",
                options=list(LABELS.keys()),
                format_func=lambda x: LABELS[x],
                index=list(LABELS.keys()).index(result['label']) if result['label'] in LABELS else 0,
                key="all_labels_radio"
            )
        
        with col_corr2:
            st.write("")  # Отступ
            st.write("")  # Отступ
            if st.button("💾 Сохранить в базу", type="secondary", use_container_width=True):
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text("INSERT INTO train.train_data (span, label, source) VALUES (:span, :label, :source)"),
                            {"span": text_input, "label": correct_label, "source": "manual"}
                        )
                    
                    # Проверяем, исправил ли пользователь метку
                    original_label = st.session_state.original_label
                    user_corrected = correct_label != original_label
                    
                    # Очищаем результаты после сохранения
                    st.session_state.last_result = None
                    st.session_state.last_text = ""
                    
                    if user_corrected:
                        # Показываем благодарность за исправление
                        st.session_state.show_thankyou = True
                        st.rerun()
                    else:
                        st.success("✅ Данные успешно сохранены в базу!")
                        st.balloons()
                        st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Ошибка при сохранении в базу: {e}")

else:
    st.error("❌ Не удалось загрузить модель для классификации.")

# Футер
st.markdown("---")
st.caption("🎯 Система классификации отзывов о чипсах | Owned by Kaftal 🥔")