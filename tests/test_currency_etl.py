from include.currency_etl import transform_rates


def test_transform_matches_required_schema():
    raw = [{"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15}]
    out = transform_rates(raw, retrieved_at="2026-11-05T07:15:00")
    assert out == [
        {
            "base_currency": "EUR",
            "target_currency": "USD",
            "exchange_rate": 1.15,
            "rate_date": "2026-11-04",
            "source": "Frankfurter API",
            "retrieved_at": "2026-11-05T07:15:00",
        }
    ]
