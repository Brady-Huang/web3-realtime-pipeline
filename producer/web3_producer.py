import os
import json
from web3 import Web3
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

WEB3_PROVIDER_URI = os.getenv("WEB3_PROVIDER_URI")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "erc20_transfer")

# Connect to Ethereum RPC node
w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_URI))
if not w3.is_connected():
    raise ConnectionError("❌ Failed to connect to Ethereum node")

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Keccak hash of ERC20 Transfer event signature
TRANSFER_EVENT_SIG = w3.keccak(text="Transfer(address,address,uint256)").hex()
if not TRANSFER_EVENT_SIG.startswith("0x"):
    TRANSFER_EVENT_SIG = "0x" + TRANSFER_EVENT_SIG

def handle_event(event):
    try:
        from_address = "0x" + event["topics"][1].hex()[-40:]
        to_address = "0x" + event["topics"][2].hex()[-40:]
        value = int.from_bytes(event["data"],byteorder='big')
        tx_hash = event["transactionHash"].hex()
    
        payload = {
            "from": from_address,
            "to": to_address,
            "value": value,
            "tx_hash": tx_hash,
            "block_number": event["blockNumber"]
        }

        print("→ Kafka Send:", payload)
        producer.send(KAFKA_TOPIC, payload)

    except Exception as e:
        print("❌ Error parsing or sending event:", e)

def main():
    print("📡 Listening for ERC20 Transfer logs...")
    transfer_filter = w3.eth.filter({
        "topics": [TRANSFER_EVENT_SIG]
    })

    while True:
        for event in transfer_filter.get_new_entries():
            handle_event(event)

if __name__ == "__main__":
    main()
