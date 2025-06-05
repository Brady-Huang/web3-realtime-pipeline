import os
import json
import asyncio
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv
from eth_abi import decode
from eth_utils import to_checksum_address
from web3 import Web3



# Load .env
load_dotenv()

INFURA_WS = os.getenv("WEB3_WS_URI")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "erc20_transfer")

sig = Web3.keccak(text="Transfer(address,address,uint256)").hex()
TRANSFER_EVENT_SIG = sig if sig.startswith("0x") else "0x" + sig
# Allow-list token contracts
ALLOWED_TOKENS = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0x6b175474e89094c44da98b954eedeac495271d0f",
    "0x514910771af9ca656af840dff83e8264ecf986ca",
    "0x853d955aCEf822Db058eb8505911ED77F175b99e",
    "0x111111111117dc0aa78b770fa6a738034120c302",
    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    "0xE41d2489571d322189246DaFA5ebDe1F4699F498",
    "0x0D8775F648430679A709E98d2b0Cb6250d2887EF"
}
# Token metadata (symbol and decimals)
with open("token_hash_map.json") as f:
    TOKEN_METADATA = json.load(f)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def consume_logs():
    async with websockets.connect(INFURA_WS) as ws:
        print("✅ Connected to Ethereum via WebSocket")

        sub_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": list(ALLOWED_TOKENS),
                    "topics": [TRANSFER_EVENT_SIG]
                }
            ]
        }

        await ws.send(json.dumps(sub_payload))
        response = await ws.recv()
        print("🔌 Subscribed:", response)

        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            try:
                log = data["params"]["result"]
                contract = log["address"].lower()
                if contract not in ALLOWED_TOKENS:
                    continue
                topics = log["topics"]
                from_addr = to_checksum_address("0x" + topics[1][-40:])
                to_addr = to_checksum_address("0x" + topics[2][-40:])
                value = int(log["data"], 16)
                meta = TOKEN_METADATA.get(contract, {"symbol": "UNKNOWN", "decimals": 18})
                human_value = value / (10 ** meta["decimals"])
                payload = {
                    "from": from_addr,
                    "to": to_addr,
                    "value": value,
                    "human_value": human_value,
                    "symbol": meta["symbol"],
                    "tx_hash": log["transactionHash"],
                    "block_number": int(log["blockNumber"], 16),
                    "contract": contract
                }

                print("→ Kafka Send:", payload)
                producer.send(KAFKA_TOPIC, payload)

            except Exception as e:
                print("❌ Error:", e)

if __name__ == "__main__":
    asyncio.run(consume_logs())
