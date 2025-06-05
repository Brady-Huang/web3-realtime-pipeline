import os
import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
from dotenv import load_dotenv

# load .env
load_dotenv()

# read env var
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ClickHouse Client
clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)

# 建表（防呆）
clickhouse_client.execute('''
    CREATE TABLE IF NOT EXISTS erc20_transfers (
        `from` String,
        `to` String,
        `value` UInt256,
        `human_value` Float64,
        `symbol` String,
        `contract` String,
        `tx_hash` String,
        `block_number` UInt64
    ) ENGINE = MergeTree()
    ORDER BY (block_number, tx_hash)
''')

print("✅ Kafka Consumer is running...")

for message in consumer:
    data = message.value

    if all(k in data for k in ("from", "to", "value", "human_value", "symbol", "contract", "tx_hash", "block_number")):
        clickhouse_client.execute('''
            INSERT INTO erc20_transfers 
            (`from`, `to`, `value`, `human_value`, `symbol`, `contract`, `tx_hash`, `block_number`)
            VALUES
        ''', [[
            data["from"],
            data["to"],
            int(data["value"]),
            float(data["human_value"]),
            data["symbol"],
            data["contract"],
            data["tx_hash"],
            int(data["block_number"])
        ]])
        print(f"✅ Inserted {data['symbol']} {data['human_value']} from {data['from']} to {data['to']}")
    else:
        print("⚠️ Incomplete data, skipping:", data)