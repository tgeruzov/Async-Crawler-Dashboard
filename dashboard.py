import streamlit as st
import pandas as pd
import sqlite3
import time
import plotly.express as px

# Настройки страницы
st.set_page_config(
    page_title="Crawler Dashboard",
    page_icon="🕷️",
    layout="wide"
)

DB_PATH = "crawler_state.db"

def load_data():
    """Читает данные из SQLite. Использует таймаут, если база заблокирована."""
    try:
        # Используем контекстный менеджер и режим read-only URI, если возможно,
        # но для SQLite достаточно просто открыть соединение
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        
        # Основная статистика
        query_main = "SELECT * FROM visited ORDER BY timestamp DESC"
        df = pd.read_sql(query_main, conn)
        
        conn.close()
        return df
    except Exception as e:
        # Если база еще не создана
        return pd.DataFrame()

# Заголовок
st.title("🕷️ Crawler Live Monitor")

# Кнопка обновления
if st.button('Обновить данные'):
    st.rerun()

# Автообновление (экспериментально)
# auto_refresh = st.checkbox('Автообновление (5 сек)', value=False)
# if auto_refresh:
#     time.sleep(5)
#     st.rerun()

df = load_data()

if df.empty:
    st.warning("База данных пуста или не найдена. Запустите crawler.")
else:
    # Преобразование даты
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # 1. Метрики (KPIs)
    col1, col2, col3, col4 = st.columns(4)
    
    total_visited = len(df)
    success_pages = len(df[df['status'] == 200])
    failed_pages = len(df[df['status'] != 200])
    avg_words = int(df[df['status'] == 200]['word_count'].mean()) if success_pages > 0 else 0
    
    col1.metric("Всего ссылок", total_visited)
    col2.metric("Успешно (200 OK)", success_pages)
    col3.metric("Ошибки", failed_pages)
    col4.metric("Ср. кол-во слов", avg_words)

    st.markdown("---")

    # 2. Графики
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Статусы ответов")
        status_counts = df['status'].value_counts().reset_index()
        status_counts.columns = ['Status Code', 'Count']
        fig_pie = px.pie(status_counts, values='Count', names='Status Code', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)

    with c2:
        st.subheader("Динамика скачивания")
        # Группировка по минутам
        df_ts = df.set_index('timestamp')
        try:
            resampled = df_ts.resample('1min').count()['url'].reset_index()
            fig_line = px.line(resampled, x='timestamp', y='url', title='Страниц в минуту')
            st.plotly_chart(fig_line, use_container_width=True)
        except Exception:
            st.info("Недостаточно данных для графика времени")

    # 3. Распределение слов
    st.subheader("Распределение длины текста (Word Count)")
    if success_pages > 0:
        fig_hist = px.histogram(df[df['status']==200], x="word_count", nbins=50)
        st.plotly_chart(fig_hist, use_container_width=True)

    # 4. Таблица последних данных
    st.subheader("Последние обработанные URL")
    st.dataframe(
        df[['timestamp', 'status', 'word_count', 'url']].head(50),
        use_container_width=True
    )