FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir "confluent-kafka>=2.4,<3"

COPY flows/cdc_trigger/prefect_event_trigger.py /app/prefect_event_trigger.py

CMD ["python", "/app/prefect_event_trigger.py"]
