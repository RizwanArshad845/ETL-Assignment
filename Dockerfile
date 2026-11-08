FROM quay.io/astronomer/astro-runtime:12.6.0

LABEL maintainer="Shafkhalid <shafkhalid10@gmail.com>" \
      org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow ETL: Frankfurter exchange rates -> MongoDB" \
      org.opencontainers.image.version="1.0.0" \
      project="Task 3 - Building Airflow ETL Pipeline"
