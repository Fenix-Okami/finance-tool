import os
import datetime as dt
import pandas as pd
from plaid import ApiClient, Configuration
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions


def _client() -> plaid_api.PlaidApi:
    """Create a Plaid client using environment variables."""
    config = Configuration(
        host=os.getenv("PLAID_ENV", "sandbox"),
        api_key={
            "clientId": os.getenv("PLAID_CLIENT_ID"),
            "secret": os.getenv("PLAID_SECRET"),
        },
    )
    return plaid_api.PlaidApi(ApiClient(config))


def fetch_recent_transactions(access_token: str, days: int = 30) -> pd.DataFrame:
    """Fetch recent transactions from Plaid."""
    client = _client()
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=days)
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        options=TransactionsGetRequestOptions(count=100),
    )
    response = client.transactions_get(request)
    data = response.to_dict().get("transactions", [])
    return pd.DataFrame(data)
