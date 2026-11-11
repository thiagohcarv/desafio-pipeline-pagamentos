#!/usr/bin/env python3
"""Consumidor de alertas de fraude — o trecho "consumer -> efeito" do pipeline.

Garantia ponta a ponta = at-least-once do Kafka + efeito idempotente aqui:
  1. lê o alerta (grupo desafio-alertas, commit MANUAL);
  2. aplica o efeito numa transação SQLite com INSERT OR IGNORE pela chave
     natural alert_id (o "bloqueio preventivo do cartão" da demo);
  3. só então faz commit do offset.
Se o processo cair entre 2 e 3, o alerta é relido e o INSERT OR IGNORE o
descarta: o efeito acontece uma única vez. Essa parte é código meu — nem o
Kafka nem o Flink garantem isso pelo consumidor.

Rode via:  ./scripts/consumir-alertas.sh   (exporta as variáveis do .state.env)
"""
import json
import os
import signal
import sqlite3
from pathlib import Path

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "desafio.fraud.detected"
GROUP = os.environ.get("ALERT_GROUP", "desafio-alertas")
DB_PATH = Path(__file__).with_name("alertas.db")

running = True


def stop(*_):
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sr = SchemaRegistryClient({
        "url": os.environ["SR_URL"],
        "basic.auth.user.info": f'{os.environ["CONSUMER_SR_KEY"]}:{os.environ["CONSUMER_SR_SECRET"]}',
    })
    deserialize = AvroDeserializer(sr)

    consumer = Consumer({
        "bootstrap.servers": os.environ["BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONSUMER_KAFKA_KEY"],
        "sasl.password": os.environ["CONSUMER_KAFKA_SECRET"],
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    db = sqlite3.connect(DB_PATH)
    db.execute("""CREATE TABLE IF NOT EXISTS bloqueios_preventivos (
        alert_id     TEXT PRIMARY KEY,
        card_id      INTEGER NOT NULL,
        total_amount TEXT NOT NULL,
        payload      TEXT NOT NULL,
        aplicado_em  TEXT NOT NULL DEFAULT (datetime('now')))""")
    db.commit()

    consumer.subscribe([TOPIC])
    print(f"ouvindo {TOPIC} como grupo {GROUP} (Ctrl+C para sair)")
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            alert = deserialize(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if alert is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            with db:  # transação: efeito todo-ou-nada
                cur = db.execute(
                    "INSERT OR IGNORE INTO bloqueios_preventivos (alert_id, card_id, total_amount, payload) "
                    "VALUES (?, ?, ?, ?)",
                    (alert["alert_id"], alert["card_id"], str(alert["total_amount"]),
                     json.dumps(alert, default=str, ensure_ascii=False)),
                )
            efeito = "EFEITO APLICADO" if cur.rowcount == 1 else "DUPLICADO — ignorado (idempotência)"
            print(f"[p{msg.partition()}@{msg.offset()}] {alert['alert_id']} "
                  f"cartão *{alert['card_last4']} {alert['tx_count']} tx R$ {alert['total_amount']} -> {efeito}")

            consumer.commit(message=msg, asynchronous=False)  # só depois do efeito
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
