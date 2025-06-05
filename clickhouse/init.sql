CREATE TABLE IF NOT EXISTS erc20_transfers (
    `timestamp` DateTime DEFAULT now(),
    `block_number` UInt32,
    `tx_hash` String,
    `from` String,
    `to` String,
    `value` UInt256,
    `human_value` Float64,
    `symbol` LowCardinality(String),
    `contract` String
)
ENGINE = MergeTree
ORDER BY (symbol, timestamp);
