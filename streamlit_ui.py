import streamlit as st

st.title('Всем привет!')

st.markdown('''
            # Здарова
            *Сейчас я буду прогонять эпохи! ;)*
            ''')

# Стилизованная HTML кнопка
st.markdown("""
<style>
.custom-button {
    display: inline-block;
    padding: 12px 24px;
    background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
    color: white;
    border: none;
    border-radius: 25px;
    font-size: 16px;
    font-weight: bold;
    cursor: pointer;
    text-decoration: none;
    text-align: center;
    box-shadow: 0 4px 15px 0 rgba(0,0,0,0.2);
    transition: all 0.3s ease;
    margin: 10px 0;
}

.custom-button:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px 0 rgba(0,0,0,0.3);
    background: linear-gradient(45deg, #FF8E8E, #6ED9D0);
}
</style>

<a href="https://www.myinstants.com/ru/instant/bombardiro-crocodilo-short-73266/?utm_source=copy&utm_medium=share" 
   target="_blank" class="custom-button">
   🎵 Включить звук крокодила! 🐊
</a>
""", unsafe_allow_html=True)

st.checkbox(label='Кликни на меня')
st.balloons()