# finance-tool

Streamlit-based tool for parsing and viewing transactions.

## Features

- Parse Bank of America and Chase PDF statements.
- Import transaction CSV files.
- Fetch recent transactions using [Plaid](https://plaid.com/).

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Configure Plaid credentials via environment variables:

```bash
export PLAID_CLIENT_ID=your_client_id
export PLAID_SECRET=your_secret
export PLAID_ENV=sandbox  # or development / production
```

## Run

Start the Streamlit app:

```bash
streamlit run app.py
```

Upload PDF or CSV statements, or provide a Plaid access token to fetch transactions.
