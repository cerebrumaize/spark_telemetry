import json
from pathlib import Path

SETTING_NAMES = [f"setting{i}" for i in range(1, 4)]
SENSOR_NAMES = [f"sensor{i}" for i in range(1, 22)]

def build_schema():
    fields = [
        {
            "name": "event_time",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        {"name": "unit_number", "type": "int"},
        {"name": "time_in_cycles", "type": "int"},
    ]
    for name in SETTING_NAMES+SENSOR_NAMES:
        fields.append({"name": name, "type": "double"})
    return {
        "type": "record",
        "name": "CMAPSSRecord",
        "namespace": "telemetry",
        "fields": fields,
    }

if __name__ == "__main__":
    out = Path(__file__).parent / "cmapss_record.avsc"
    out.write_text(json.dumps(build_schema(), indent=2))
    print(f"wrote {out}")