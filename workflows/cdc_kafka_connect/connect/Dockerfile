FROM confluentinc/cp-kafka-connect:7.6.1

USER root
RUN confluent-hub install --no-prompt snowflakeinc/snowflake-kafka-connector:latest
USER appuser
