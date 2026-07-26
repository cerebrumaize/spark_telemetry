import time
import json
from confluent_kafka import Producer
from pathlib import Path

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
    #1 1 -0.0007 -0.0004 100.0 518.67 641.82 1589.70 1400.60 14.62 21.61 
    #554.36 2388.06 9046.19 1.30 47.47 521.66 2388.02 8138.62 8.4195 0.03 392
    #2388 100.00 39.06 23.4190  
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
    producer_config = {
        "bootstrap.servers": "localhost:19092"
    }

    producer = Producer(producer_config)
    with open(DATA_PATH, "r") as f:
        for line in f:
            record = parse_line_to_record(line)
            value = json.dumps(record).encode("utf-8")
            producer.produce(
                topic="sensor.raw",
                key=str(record["unit_number"]).encode("utf-8"),
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)  # Trigger delivery report callbacks
    producer.flush()

if __name__ == "__main__":
    main()