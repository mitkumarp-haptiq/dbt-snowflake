"""Bridge Kafka CDC activity to a Prefect Automation webhook.

Architecture:
    Debezium -> Redpanda topic -> THIS BRIDGE -> Prefect webhook automation.

The bridge consumes topic messages (with its own observer group) and emits one
webhook call per cooldown window when new messages are observed.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group_id: str
    cooldown_seconds: int
    webhook_url: str
    request_timeout_s: int


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def load_config() -> Config:
    webhook_url = (os.environ.get("PREFECT_CDC_WEBHOOK_URL") or "").strip()
    if not webhook_url:
        raise ValueError(
            "PREFECT_CDC_WEBHOOK_URL must be set for webhook trigger mode."
        )
    return Config(
        kafka_bootstrap=os.environ.get("CDC_KAFKA_BOOTSTRAP", "redpanda:29092"),
        kafka_topic=os.environ.get("CDC_KAFKA_TOPIC", "app.public.users"),
        kafka_group_id=os.environ.get(
            "CDC_TRIGGER_GROUP_ID", "cdc-prefect-webhook-bridge"
        ),
        cooldown_seconds=_env_int("CDC_TRIGGER_COOLDOWN_SECONDS", 120),
        webhook_url=webhook_url,
        request_timeout_s=_env_int("CDC_TRIGGER_HTTP_TIMEOUT_SECONDS", 10),
    )


def _post_webhook(cfg: Config, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url=cfg.webhook_url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=cfg.request_timeout_s) as resp:
            _ = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Webhook HTTP {exc.code} for {cfg.webhook_url}: {body}") from exc


def main() -> None:
    cfg = load_config()
    print(
        "cdc_webhook_bridge_start "
        f"topic={cfg.kafka_topic} group={cfg.kafka_group_id} "
        f"cooldown_s={cfg.cooldown_seconds}",
        flush=True,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "group.id": cfg.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "session.timeout.ms": 30_000,
        }
    )
    consumer.subscribe([cfg.kafka_topic])

    pending_count = 0
    last_trigger_at = 0.0

    try:
        while True:
            now = time.monotonic()
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"cdc_webhook_bridge_poll_error error={msg.error()}", flush=True)
                    continue
                pending_count += 1

            has_new_since_last_trigger = pending_count > 0
            cooldown_elapsed = (now - last_trigger_at) >= cfg.cooldown_seconds
            if has_new_since_last_trigger and cooldown_elapsed:
                try:
                    payload = {
                        "event": "cdc.kafka.topic_activity",
                        "topic": cfg.kafka_topic,
                        "observed_messages": pending_count,
                        "observed_at_epoch_s": int(time.time()),
                    }
                    _post_webhook(cfg, payload)
                    consumer.commit(asynchronous=False)
                    last_trigger_at = now
                    sent_count = pending_count
                    pending_count = 0
                    print(
                        f"cdc_webhook_bridge_fired topic={cfg.kafka_topic} "
                        f"observed_messages={sent_count}",
                        flush=True,
                    )
                except Exception as exc:
                    print(f"cdc_webhook_bridge_fire_error error={exc}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
