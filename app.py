import streamlit as st
from parse import parse_pdf, parse_csv
from plaid_client import fetch_recent_transactions

st.title("Finance Tool")

uploaded_file = st.file_uploader(
    "Upload a bank statement PDF or transactions CSV", type=["pdf", "csv"]
)

if uploaded_file:
    if uploaded_file.name.lower().endswith(".pdf"):
        try:
            df = parse_pdf(uploaded_file)
            st.success("Parsed PDF statement")
        except Exception as e:
            st.error(f"Failed to parse PDF: {e}")
            df = None
    else:
        df = parse_csv(uploaded_file)
        st.success("Loaded CSV file")

    if df is not None:
        st.dataframe(df)

st.header("Plaid")
access_token = st.text_input("Plaid access token", type="password")
if st.button("Fetch recent transactions"):
    if access_token:
        try:
            plaid_df = fetch_recent_transactions(access_token)
            st.dataframe(plaid_df)
        except Exception as e:
            st.error(f"Plaid error: {e}")
    else:
        st.warning("Please provide an access token")
