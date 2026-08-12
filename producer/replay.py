import copy
import argparse, itertools
import json
from pathlib import Path
import random
import time
from confluent_kafka import SerializingProducer, Producer
from confluent_kafka.error import ValueSerializationError
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import heapq

DATA_PATH = Path(__file__).parent.parent / "data" / "CMAPSSData" /"train_FD001.txt"
BASE_EPOCH_MS = 1_785_007_500_000  # 2026-07-25 12:25:00 UTC in milliseconds

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Delivery succeeded for record {msg.key()} produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def build_event_time(cycle, interval_ms):
    return BASE_EPOCH_MS + cycle * interval_ms

def parse_line_to_record(line, interval_ms):
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

def to_dlq(dlq_producer, line_num, raw, error, stage):
    dlq_record = {
        "line_num": line_num,
        "raw_line": raw,
        "error": str(error),
        "stage": stage,
        "processed_time": int(time.time() * 1000)  # current time in milliseconds
    }
    dlq_producer.produce(
        topic="sensor.raw.dlq",
        key=str(line_num),
        value=json.dumps(dlq_record).encode("utf-8"),
        on_delivery=delivery_report,
    )

def main(args):
    speed = args.speed
    interval_ms = args.interval_ms
    chaos = args.chaos
    chaos_late_rate = float(args.late_rate) if chaos else 0.0
    chaos_late_max_ms = int(args.late_max_ms) if chaos else 0
    chaos_dup_rate = float(args.dup_rate) if chaos else 0.0
    chaos_corrupt_rate = float(args.corrupt_rate) if chaos else 0.0
    chaos_seed = int(args.seed) if chaos else 0

    counter = itertools.count()  # for tie-breaking in heapq

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
    dlq_producer = Producer({"bootstrap.servers": "localhost:19092"})

    records = []
    t0_event_ms = BASE_EPOCH_MS + interval_ms  # because I know the min event_time is cycle 1
    t0_wall_ms = int(time.time() * 1000) # when program starts, in milliseconds
    rand = random.Random(chaos_seed)  # for reproducibility

    # read in the data file and sort by event_time
    with open(DATA_PATH, "r") as f:
        for line_num, line in enumerate(f, start=1):
            # layer 1: parse line to record
            try:
                record = parse_line_to_record(line, interval_ms)
                # record["unit_number"] = str(record["unit_number"])  # convert to str to test avro

                # late events manipulation
                normal_send_wall = t0_wall_ms + (record["event_time"] - t0_event_ms) / speed
                if chaos and rand.random() <= chaos_late_rate:
                    actual_send_wall = normal_send_wall + (rand.randint(0, chaos_late_max_ms) / speed)
                else:
                    actual_send_wall = normal_send_wall

                # duplicate events injection
                if chaos and rand.random() <= chaos_dup_rate:
                    dup_send_wall = actual_send_wall + (rand.randint(0, 100) / speed) # small delay for duplicates
                    records.append((dup_send_wall, next(counter), record, line_num, line.strip()))

                # normal events injection
                records.append((actual_send_wall, next(counter), record, line_num, line.strip()))

            except (ValueError, IndexError) as e:
                to_dlq(dlq_producer, line_num, line.strip(), e, stage="parse")
                continue
    heapq.heapify(records)  # sort by actual_send_wall, tiebreaker

    # replay with wall-clock pacing
    while records:
        actual_send_wall, tie_breaker, record, line_num, raw_line = heapq.heappop(records)
        delay_ms = actual_send_wall - time.time() * 1000
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0) # sleep expects seconds

        if chaos and rand.random() <= chaos_corrupt_rate:
            record = copy.deepcopy(record)  # make a copy to avoid mutating the original
            record["unit_number"] = str(record["unit_number"]) + "_corrupt"  # corrupt the unit_number to test avro serialization error

        # layer 2: avro serilize + append to kafka
        try:
            producer.produce(
                topic="sensor.raw",
                key=str(record["unit_number"]),  # raw str, serializer handles bytes
                value=record,                    # raw dict, avro serializer handles it
                on_delivery=delivery_report,     # note: on_delivery, not callback
            )
        except ValueSerializationError as e:
            to_dlq(dlq_producer, line_num, raw_line, e, stage="serialize")
            continue

        producer.poll(0)  # Trigger delivery report on_delivery
        dlq_producer.poll(0)  # Trigger delivery report on_delivery for DLQ producer

    producer.flush()
    dlq_producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="replay_cmapss",
        description="Replay CMAPSS data to Kafka with wall-clock pacing.")
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--interval-ms", type=int, default=1000)

    parser.add_argument("--chaos", action="store_true")   # 不加chaos就默认False,加了--chaos就是True
    parser.add_argument("--late-rate", type=float, default=0.0)
    parser.add_argument("--late-max-ms", type=int, default=0)
    parser.add_argument("--dup-rate", type=float, default=0.0)
    parser.add_argument("--corrupt-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=0)

    args = parser.parse_args()

    main(args)