import streamlit as st
from smart_system import run_analysis
from datetime import datetime

st.title("News Bias Analysis System")
st.write("Compare BD vs International media narratives")

st.markdown("### Select Analysis Parameters")

keyword = st.text_input(
    "Keyword (optional)",
    placeholder="e.g. Russia, Ukraine, Hormuz, ceasefire"
)

topic = st.selectbox(
    "Select Topic (optional)",
    [
        "All",
        "Russia Ukraine war",
        "Iran Israel war",      
        "Taiwan strait conflict"
    ]
)

start_date = st.date_input("Start Date", value=datetime(2026, 4, 11))
end_date = st.date_input("End Date", value=datetime(2026, 4, 13))

if start_date > end_date:
    st.error("Start date cannot be after end date")
    st.stop()

if st.button("Run Analysis"):
    with st.spinner("Analyzing news and detecting bias..."):
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        result = run_analysis(
            keyword=keyword.strip() if keyword and keyword.strip() else None,
            topic=topic if topic != "All" else None,
            start_date=start_str,
            end_date=end_str
        )

    st.subheader("Bias Analysis Result")
    st.markdown(result)