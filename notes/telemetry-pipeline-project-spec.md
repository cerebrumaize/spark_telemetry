# Sensor Telemetry Streaming Pipeline — Project Spec

## Goal

Close the streaming and lakehouse gap with one project that is defensible in an interview. Target: someone who reads the repo believes you have built a stateful streaming pipeline end to end, understands event-time semantics, and has thought about what breaks in production.

Not the goal: a tutorial walkthrough. The differentiator is the failure handling and the writeup, not the happy path.

---

## Dataset

**Primary: NASA C-MAPSS turbofan engine degradation.**

Why this one:
- Real sensor semantics. 21 sensor channels plus 3 operational settings per engine per cycle, with run-to-failure trajectories across ~100–250 engine units.
- Multi-device by construction. Each engine unit is an independent device emitting its own series, which is exactly the shape of fleet telemetry (drones, robots, grid sensors, medical devices).
- Degradation is the signal. Engines run normally, then a fault appears and worsens until failure. That gives you something real to detect rather than a synthetic threshold.
- It maps directly onto the domains you want. Predictive maintenance on a device fleet is the same problem shape as Zipline's drone telemetry, Gridware's grid sensors, and Intrinsic's robot fleets.

Available on Kaggle and NASA's prognostics data repository. FD001 is the simplest subset (one operating condition, one fault mode); FD004 is the hardest (six conditions, two fault modes). Start with FD001, move to FD004 in phase 3 to exercise schema and logic complexity.

**Secondary, optional: a live public stream** to prove the pipeline handles unbounded real input. The bytewax `awesome-public-real-time-datasets` list has options. Only add this if phases 1–4 land comfortably.

### Making it a stream

C-MAPSS ships as static CSV. You write a replay producer that emits rows to Kafka on a compressed timeline (e.g. 1 cycle = 100ms), preserving the original event timestamps in the payload.

This is deliberate, not a shortcut, and say so in the README: replaying a real dataset gives you authentic sensor distributions and degradation patterns while letting you control stream behavior precisely enough to demonstrate watermarking and recovery. A pure random generator would not have the first property; a live firehose would not have the second.

---

## Architecture

```
C-MAPSS CSVs
    │
    ▼
replay producer (Python)  ──────► Kafka topic: sensor.raw
    │  emits with original event_time                │
    │  injects late/out-of-order/duplicate events    │
    │                                                ▼
    │                              Spark Structured Streaming
    │                                  - event-time windowing
    │                                  - watermarking
    │                                  - stateful aggregation per engine
    │                                  - checkpointing to disk
    │                                                │
    │                    ┌───────────────────────────┼───────────────────┐
    │                    ▼                           ▼                   ▼
    │            Iceberg: bronze            Iceberg: silver       Iceberg: gold
    │            (raw, append-only)         (cleaned, deduped)    (per-engine
    │                                                              health windows)
    │                                                                    │
    │                                                                    ▼
    └──────────────────────────────────────────────────► dbt models + tests
                                                                         │
                                                                         ▼
                                                            Grafana / Streamlit
                                                            (fleet health view)
```

Everything runs locally in Docker Compose. No cloud spend required.

---

## Phases

Assume roughly 4–5 hours a day on the project. Phase durations below are working days, not calendar days.

### Phase 1 — Ingest and replay (3–4 days)

Build the producer and get bytes flowing.

- Docker Compose with Kafka (or Redpanda, which is lighter and Kafka-API-compatible) and Kafka UI.
- Python replay producer: reads C-MAPSS, emits one message per engine-cycle, keyed by `engine_id`, carrying `event_time`, `unit`, `cycle`, operational settings, and 21 sensor values.
- Configurable replay speed and a `--chaos` flag that will later inject disorder.
- Schema defined explicitly. Use Avro or Protobuf with a schema registry rather than raw JSON — Protobuf appears in Gridware's bonus list and schema registry is a real production concern most side projects skip.

**Deliverable:** producer emits a controllable, schema-validated stream. You can watch it in Kafka UI.

### Phase 2 — Stateful streaming with Spark (5–7 days)

This is the phase that closes the actual gap. Do not rush it.

- Spark Structured Streaming job reading `sensor.raw`.
- **Event-time windowing:** tumbling and sliding windows over sensor readings per engine.
- **Watermarking:** set an explicit watermark, then use the producer's chaos mode to emit events beyond it and observe them being dropped. Tune the watermark and show the tradeoff between completeness and state size.
- **Stateful aggregation:** running degradation metrics per engine that persist across micro-batches — rolling mean and standard deviation per sensor, cycles-since-first-anomaly, cumulative drift from the engine's own healthy baseline.
- **Checkpointing:** enable it, then kill the job mid-stream and restart. Show it resumes from the checkpoint without reprocessing or losing state. Document exactly what the checkpoint contains.
- **Deduplication:** `dropDuplicates` with watermark, exercised by injecting duplicate events.

**Deliverable:** a job that survives being killed, handles late and duplicate data explicitly, and maintains real state. Write down what you observed at each step — this is the raw material for the README.

### Phase 3 — Lakehouse (4–5 days)

- Spark writes to Iceberg tables in bronze/silver/gold layers.
- **Bronze:** append-only raw landing, no transformation.
- **Silver:** deduplicated, typed, validated. Late arrivals reconciled here.
- **Gold:** per-engine windowed health metrics, ready to query.
- **Table maintenance:** partition by date and engine, run compaction on small files, expire old snapshots. Small-file accumulation is the classic streaming-to-lakehouse problem and handling it explicitly is a strong signal.
- **Schema evolution:** move from FD001 to FD004, which has different operating conditions. Add columns to a live table and show downstream models surviving it.
- **Time travel:** demonstrate querying a prior snapshot.

**Deliverable:** a working lakehouse with layered tables, plus documented evidence you handled compaction and schema change rather than just writing files.

### Phase 4 — Transformation, quality, and view (4–5 days)

- dbt models on the gold layer for fleet-level aggregates and per-engine RUL-adjacent features. Reuse the structure from your existing dbt project.
- dbt tests: freshness on the streaming source, referential integrity between engine dimension and readings, distribution checks on sensor values.
- Anomaly rule: flag an engine when sensor drift crosses N standard deviations from its own baseline for M consecutive windows. Keep it simple and explainable — this is a data engineering project, not an ML project. Note in the README that a model would go here and why you deliberately kept the detection logic transparent.
- Grafana or a small Streamlit app: fleet health overview, per-engine drilldown, pipeline lag and throughput metrics.

**Deliverable:** something you can screenshot. The dashboard is what makes the project legible in ten seconds to a recruiter.

### Phase 5 — Documentation and failure writeup (2–3 days)

The highest-leverage phase. Your dbt project already proved this: the notes were far stronger than the README.

Write up:
- **What breaks and why.** Watermark too tight, data lost. Too loose, state grows unbounded. Small files degrade query performance. Checkpoint incompatible after code change — this one bites people in production and is worth reproducing deliberately.
- **Tradeoffs you chose.** Why Spark over Flink here. Why Iceberg. Why replay over live stream. Why rule-based detection over a model.
- **What would change at real scale.** State backend limits, partitioning strategy at 10k devices instead of 100, exactly-once vs at-least-once and what your sink actually guarantees.

Structure the README like the one we wrote for dbt_duckdb: what's implemented, problems worth noting, how to run it. Put the deep material in a separate notes file.

**Total: roughly 18–24 working days.** At 4–5 hours a day alongside job searching, that is about five to six calendar weeks.

---

## Phase 6 (later) — Flink

Once phases 1–5 are done and the repo is presentable, port the phase 2 job to Flink and write a comparison.

This is the highest-value optional addition, because "I implemented the same stateful pipeline in both Spark Structured Streaming and Flink and here is where they differ" is a genuinely differentiating claim. Very few candidates can say it. Focus the comparison on:
- True streaming vs micro-batch and what that means for latency
- State backend differences (RocksDB in Flink)
- Checkpointing and exactly-once mechanics
- When you would actually reach for each

Do not start this before phase 5 ships. An unfinished project helps nobody.

---

## Notes on execution

**Commit continuously and write as you go.** Do not save documentation for the end. When you hit a problem in phase 2, write the note that day, the way you did in `project-notes.md`. Reconstructing it three weeks later loses the specifics that make it credible.

**Resist scope creep toward ML.** RUL prediction on C-MAPSS is a well-trodden ML problem and it is tempting. It is not what you are being hired for and it will eat the time budget. Keep detection rule-based and say why.

**Ship each phase as a working state.** If the search produces an offer at week three, you want phases 1–2 done and documented rather than five phases half-built.

**On the resume**, this replaces the standalone Kafka bullet. It becomes the lead project: a stateful streaming telemetry pipeline with lakehouse storage, not three unrelated exercises.
