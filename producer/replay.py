import optparse
import json
from pathlib import Path
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

DATA_PATH = Path(__file__).parent.parent / "data" / "CMAPSSData" /"train_FD001.txt"
BASE_EPOCH_MS = 1_785_007_500_000  # 2026-07-25 12:25:00 UTC in milliseconds

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Delivery succeeded for record {msg.key()} produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def build_event_time(cycle, interval_ms):
    return BASE_EPOCH_MS + cycle * interval_ms

def parse_line_to_record(line, interval_ms=1000):
    """ one record in train_FD001:
    # 1 1  # unit_number, time_in_cycles
    # -0.0007 -0.0004 100.0  # setting1, setting2, setting3
    # 518.67 641.82 1589.70 1400.60 14.62 21.61 554.36 2388.06 9046.19 1.30
    # 47.47 521.66 2388.02 8138.62 8.4195 0.03 392 2388 100.00 39.06
    # 23.4190  
    """
    sensor_name = [
        "sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7",
        "sensor8", "sensor9", "sensor10", "sensor11", "sensor12", "sensor13", 
        "sensor14", "sensor15", "sensor16", "sensor17", "sensor18", "sensor19",
        "sensor20", "sensor21"
    ]

    values = line.split()
    cycle = int(values[1])  # the second value is the cycle number
    record = {
        "event_time": build_event_time(cycle, interval_ms),
        "unit_number": int(values[0]),
        "time_in_cycles": cycle,
        "setting1": float(values[2]),
        "setting2": float(values[3]),
        "setting3": float(values[4]),
    }
    for i, name in enumerate(sensor_name):
            record[name] = float(values[i+5])

    return record

def main():
    schema_registry_conf = {"url": "http://localhost:18081"}  # host 从宿主机连
    sr_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = (Path(__file__).parent / "schemas" / "cmapss_record.avsc").read_text()

    avro_serializer = AvroSerializer(
        sr_client,
        schema_str,                      # sensor_reading.avsc 的内容
        lambda obj, ctx: obj,            # record already a dict, pass through
    )
    string_serializer = StringSerializer("utf_8")
    
    producer = SerializingProducer({
        "bootstrap.servers": "localhost:19092",
        "key.serializer": string_serializer,
        "value.serializer": avro_serializer
    })

    with open(DATA_PATH, "r") as f:
        for line in f:
            record = parse_line_to_record(line)
            producer.produce(
                topic="sensor.raw",
                key=str(record["unit_number"]),  # raw str, serializer handles bytes
                value=record,                    # raw dict, avro serializer handles it
                # callback=delivery_report,
                on_delivery=delivery_report,     # note: on_delivery, not callback
            )
            producer.poll(0)  # Trigger delivery report callbacks
    producer.flush()

if __name__ == "__main__":
    main()