# Streaming from Kafka

Two notebooks, the same 8 checks:

- `kafka_streaming.ipynb`: LakeLogic's `StreamSink` reads Kafka and runs each micro-batch on Polars, DuckDB or Spark.
- `spark_structured_streaming.ipynb`: **Spark reads Kafka itself** (`spark.readStream`, Spark's checkpoint), and each micro-batch runs the contract in `foreachBatch` (`SparkStreamSink` + `kafka_json_stream`). This is the Databricks and Fabric path. Needs PySpark 3.5 + `delta-spark`; Spark downloads its Kafka connector on first run. Checks only: `python structured_scenarios.py`.

**The contracts** — everything a run needs is in the contract; no Python wiring:

- [`contracts/rides_kafka.yaml`](contracts/rides_kafka.yaml): `source.type: stream`, `options.kind: kafka`, brokers (`env:KAFKA_BROKERS`), topic, checkpoint, batch size, trigger, optional SASL (`sasl_password: env:VAR`).
- [`contracts/rides_eventhubs.yaml`](contracts/rides_eventhubs.yaml): `kind: eventhubs` + `connection_string: env:EVENTHUBS_CONNECTION_STRING`; brokers and SASL come from the connection string.

`DataProcessor(engine=..., contract="contracts/rides_kafka.yaml").run_source()` (or the pipeline runner) drains the topic: Polars/DuckDB through `StreamSink` + `KafkaOffsetSource`, Spark through Structured Streaming. A secret written literally in a contract is refused.


`kafka_streaming.ipynb` runs LakeLogic's `StreamSink` against a **real** Kafka broker (Redpanda in Docker). It covers:

- draining a topic: good rows to bronze, bad rows to quarantine with their reasons;
- a later run reading only the new events;
- crashing mid-run and between the write and the checkpoint commit, with no gap and no duplicate;
- replaying the whole topic into a `merge` target;
- a message that is not JSON (quarantined; the stream carries on);
- the same ride updated twice in one batch (the last event wins, counted as `superseded`).

## Run it

```
docker run -d --name lakelogic-kafka-dev -p 19092:19092 docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
  redpanda start --mode dev-container --smp 1 \
  --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
  --advertise-kafka-addr internal://localhost:9092,external://localhost:19092
pip install "lakelogic[kafka]" deltalake
```

Open the notebook and run all cells. Or run only the checks: `ENGINE=polars python kafka_scenarios.py` (also `duckdb` or `spark`).
Spark also needs `delta-spark` matching your Spark version.

Each scenario creates its own topic, so runs never interfere. Their Delta tables and checkpoints are written under `data/stream_runs/`.
When you're done, `docker rm -f lakelogic-kafka-dev` removes the broker.

`make_notebook.py` regenerates the notebook from source.

## Azure Event Hubs

The same scripts run against Event Hubs' Kafka endpoint (Standard tier or higher; Basic has no Kafka):

```
export KAFKA_BROKERS=<namespace>.servicebus.windows.net:9093
export KAFKA_SASL_CONNECTION_STRING="$(az eventhubs namespace authorization-rule keys list -g <rg>     --namespace-name <namespace> --name RootManageSharedAccessKey --query primaryConnectionString -o tsv)"
export EH_RESOURCE_GROUP=<rg> EH_NAMESPACE=<namespace>   # lets each scenario delete its event hubs
ENGINE=polars python kafka_scenarios.py
python structured_scenarios.py
```

Each scenario deletes its event hubs afterwards, because Standard allows 10 per namespace. On Databricks, the Spark
Kafka login module is shaded: use `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`.
