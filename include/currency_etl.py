"""Pure ETL helpers for the Frankfurter currency pipeline (no Airflow imports, easy to test)."""
from datetime import datetime, timezone

import requests

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]
SOURCE_NAME = "Frankfurter API"


def extract_rates(base=BASE_CURRENCY, quotes=TARGET_CURRENCIES, timeout=30):
    """Call the Frankfurter API and return the raw JSON list.

    Example response item: {"date": "2026-11-08", "base": "EUR", "quote": "USD", "rate": 1.121}
    """
    response = requests.get(
        API_URL,
        params={"base": base, "quotes": ",".join(quotes)},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list) or not data:
        raise ValueError(f"Unexpected API response: {data!r}")
    return data


def transform_rates(raw_rates, retrieved_at=None):
    """Map raw API items to the required output schema."""
    retrieved_at = retrieved_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed = []
    for item in raw_rates:
        missing = {"date", "base", "quote", "rate"} - item.keys()
        if missing:
            raise ValueError(f"Record missing fields {missing}: {item!r}")
        transformed.append(
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": SOURCE_NAME,
                "retrieved_at": retrieved_at,
            }
        )
    return transformed
