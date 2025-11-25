import streamlit as st
from transformers import pipeline

# ==============================
# Загружаем модель
# ==============================
@st.cache_resource
def load_model():
    classifier = pipeline(
        'text-classification',
        model='./best_model_transformer',
        tokenizer='./best_model_tokenaiser'
    )
    return classifier

model = load_model()

# ==============================
# UI
# ==============================
st.set_page_config(page_title="Text Classification", layout="wide")
st.title("📝 Классификация текста с RuBERT")

st.markdown(
    """
    Введите текст в поле ниже, и модель предскажет категорию.
    """
)

user_input = st.text_area("Введите текст для классификации:", height=150)

if st.button("Предсказать"):
    if not user_input.strip():
        st.warning("Пожалуйста, введите текст для анализа!")
    else:
        with st.spinner("Выполняется инференс..."):
            results = model([user_input])
        
        st.success("✅ Предсказание готово!")
        for res in results:
            st.write(f"**Предсказанная метка:** {res['label']}")
            st.write(f"**Вероятность:** {res['score']:.4f}")

# ==============================
# Можно добавить мульти-текст
# ==============================
st.markdown("---")
st.subheader("Пакетная классификация")
batch_input = st.text_area("Введите несколько текстов (по одному на строку):", height=200)

if st.button("Предсказать пакетно"):
    lines = [l for l in batch_input.split("\n") if l.strip()]
    if not lines:
        st.warning("Введите хотя бы один текст!")
    else:
        with st.spinner("Выполняется пакетный инференс..."):
            batch_results = model(lines)
        for i, res in enumerate(batch_results):
            st.write(f"{i+1}. **Текст:** {lines[i]}")
            st.write(f"   **Метка:** {res['label']}, **Вероятность:** {res['score']:.4f}")
