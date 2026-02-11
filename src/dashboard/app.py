import streamlit as st
import pandas as pd
import json
import threading
import queue
import time
from kafka import KafkaConsumer
import plotly.graph_objects as go

# Page Config
st.set_page_config(page_title="Crypto Monitor 2026", layout="wide")

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "aggregated_crypto_stats"

# 1. Thread-safe Communication Layer
if 'data_queue' not in st.session_state:
    st.session_state.data_queue = queue.Queue()

# 2. Background Consumer Logic
def consumer_thread_logic(data_queue):
    """Runs in background thread. Puts Kafka msgs into shared queue."""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # Unique group ID ensures we don't conflict with other viewers
            group_id=f"streamlit-viewer-{time.time()}" 
        )
        for msg in consumer:
            data_queue.put(msg.value)
    except Exception as e:
        print(f"Consumer Error: {e}")

# 3. Start Background Thread (Singleton Pattern)
if 'consumer_thread' not in st.session_state:
    # We pass the queue explicitly to avoid thread context issues
    t = threading.Thread(target=consumer_thread_logic, args=(st.session_state.data_queue,), daemon=True)
    t.start()
    st.session_state.consumer_thread = t
    # Initialize dataframe structure
    st.session_state.df = pd.DataFrame(columns=['start_time', 'open', 'high', 'low', 'close', 'volatility', 'trade_count'])

# 4. State Update Function
def update_state():
    """Drains the queue and updates the Pandas DataFrame."""
    # FIX: Initialize the list properly
    new_data = []
    
    # Drain the queue of all pending messages
    while not st.session_state.data_queue.empty():
        try:
            new_data.append(st.session_state.data_queue.get_nowait())
        except queue.Empty:
            break
    
    if new_data:
        df_new = pd.DataFrame(new_data)
        # Ensure timestamps are datetime objects for Plotly
        if 'start_time' in df_new.columns:
            df_new['start_time'] = pd.to_datetime(df_new['start_time'])
        
        # Merge with existing state
        current_df = st.session_state.df
        combined = pd.concat([current_df, df_new])
        
        # Deduplication: Spark Update mode emits multiple partial results for the same window.
        # We keep the 'last' entry for each timestamp to get the most up-to-date values.
        combined = combined.drop_duplicates(subset=['start_time'], keep='last')
        
        # Trim history to keep memory usage low (last 50 candles)
        st.session_state.df = combined.sort_values('start_time').tail(50)

# 5. UI Layout
st.title("BTC/USDT Real-Time Monitor")
st.markdown("Architecture: **Binance -> Kafka KRaft -> Spark 4.0 -> Streamlit**")

# Create placeholders for metrics and charts
metric_cols = st.columns(4)
chart_placeholder = st.empty()

# 6. Reactive Dashboard Loop
# This refreshes the UI components below it every 1 second
if st.button("Refresh View"): 
    st.rerun()

update_state()
df = st.session_state.df

if not df.empty:
    latest = df.iloc[-1]
    
    with metric_cols[0]:
        st.metric("Price", f"${latest['close']:.2f}")
    with metric_cols[1]:
        st.metric("High", f"${latest['high']:.2f}")
    with metric_cols[2]:
        st.metric("Volatility", f"{latest['volatility']:.2f}")
    with metric_cols[3]:
        st.metric("Trades/Min", int(latest['trade_count']))
        
    # Candlestick Chart
    fig = go.Figure(data=[go.Candlestick(
        x=df['start_time'],
        open=df['open'], 
        high=df['high'],
        low=df['low'], 
        close=df['close']
    )])
    
    fig.update_layout(
        height=500, 
        margin=dict(l=20, r=20, t=30, b=20),
        title="1-Minute Candle Stream",
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )
    chart_placeholder.plotly_chart(fig, use_container_width=True)
else:
    st.info("Waiting for data from Spark... (This can take up to 60 seconds for the first window)")
    time.sleep(1)
    st.rerun()