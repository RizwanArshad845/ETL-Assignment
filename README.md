# Currency Exchange ETL — Airflow (Astro CLI) → MongoDB

Daily ETL pipeline that pulls EUR exchange rates (USD, GBP, JPY) from the
[Frankfurter API](https://frankfurter.dev/), transforms them, and loads them into MongoDB.
Mongo Express provides a web UI for the database.

## Architecture

```
Frankfurter API ──► extract_exchange_rates ──► transform_exchange_rates ──► load_to_mongodb ──► MongoDB ◄── Mongo Express
                  XCom key: raw_exchange_rates   XCom key: transformed_exchange_rates
```

| Task | What it does | Named XCom |
|---|---|---|
| `extract_exchange_rates` | `GET https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY` | pushes `raw_exchange_rates` |
| `transform_exchange_rates` | Maps each record to the required schema | pulls `raw_exchange_rates`, pushes `transformed_exchange_rates` |
| `load_to_mongodb` | Upserts into `currency_db.exchange_rates` (unique on base/target/date, so reruns don't duplicate) | pulls `transformed_exchange_rates` |

Transformed document:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.121,
  "rate_date": "2026-11-08",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-08T20:20:41"
}
```

## Project layout

```
dags/currency_etl_dag.py      # DAG: 3 PythonOperators + named XComs
include/currency_etl.py       # extract/transform logic (plain Python)
tests/test_currency_etl.py    # unit test for the transform schema
Dockerfile                    # Astro Runtime image + LABEL metadata
docker-compose.override.yml   # MongoDB + Mongo Express services, network, volume, labels
airflow_settings.yaml         # Airflow connection `mongo_default`
requirements.txt              # apache-airflow-providers-mongo, requests
```

## Docker configuration

- **Metadata:** `LABEL` entries in the `Dockerfile` (maintainer, title, description, version) and `labels:` on the `mongo` and `mongo-express` services.
- **Networking:** MongoDB and Mongo Express join Astro's `airflow` bridge network, so Airflow reaches the DB at hostname `mongo`.
- **Persistent storage:** the named volume `currency_mongo_data` is mounted at `/data/db`, so data survives `astro dev stop` and restart.

## Run it

Prerequisites: Docker Desktop and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
(Windows: `winget install -e --id Astronomer.Astro`).

```bash
astro dev start
```

| Service | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Mongo Express | http://localhost:8081 | admin / admin |
| MongoDB | mongodb://root:example@localhost:27017/?authSource=admin | |

1. In the Airflow UI, unpause **`currency_exchange_etl`** and trigger it.
2. Open each task → **XCom** tab to see the named keys.
3. In Mongo Express, open `currency_db` → `exchange_rates`.

Useful checks:

```bash
docker inspect --format '{{json .Config.Labels}}' currency-mongo   # labels
docker network ls                                                   # network
docker volume inspect currency_mongo_data                           # volume
astro dev pytest                                                    # tests
```

Stop the stack with `astro dev stop`. Data is kept; `astro dev kill` deletes the Airflow containers.
