Question: Explain this code snippet:

```
name: redpanda-quickstart-one-broker
networks:
  redpanda_network:
    driver: bridge
volumes:
  redpanda-0: null
services:
  redpanda-0:
    command:
      - redpanda
      - start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      # Address the broker advertises to clients that connect to the Kafka API.
      # Use the internal addresses to connect to the Redpanda brokers'
      # from inside the same Docker network.
      # Use the external addresses to connect to the Redpanda brokers'
      # from outside the Docker network.
      - --advertise-kafka-addr internal://redpanda-0:9092,external://localhost:19092
      - --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082
      # Address the broker advertises to clients that connect to the HTTP Proxy.
      - --advertise-pandaproxy-addr internal://redpanda-0:8082,external://localhost:18082
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
      # Redpanda brokers use the RPC API to communicate with each other internally.
      - --rpc-addr redpanda-0:33145
      - --advertise-rpc-addr redpanda-0:33145
      # Mode dev-container uses well-known configuration properties for development in containers.
      - --mode dev-container
      # Tells Seastar (the framework Redpanda uses under the hood) to use 1 core on the system.
      - --smp 1
      - --default-log-level=info
    image: docker.redpanda.com/redpandadata/redpanda:v26.1.13
    container_name: redpanda-0
    volumes:
      - redpanda-0:/var/lib/redpanda/data
    networks:
      - redpanda_network
    ports:
      - 18081:18081
      - 18082:18082
      - 19092:19092
      - 19644:9644
  console:
    container_name: redpanda-console
    image: docker.redpanda.com/redpandadata/console:v3.8.0
    networks:
      - redpanda_network
    entrypoint: /bin/sh
    command: -c 'echo "$$CONSOLE_CONFIG_FILE" > /tmp/config.yml; /app/console'
    environment:
      CONFIG_FILEPATH: /tmp/config.yml
      CONSOLE_CONFIG_FILE: |
        kafka:
          brokers: ["redpanda-0:9092"]
        schemaRegistry:
          enabled: true
          urls: ["http://redpanda-0:8081"]
        redpanda:
          adminApi:
            enabled: true
            urls: ["http://redpanda-0:9644"]
    ports:
      - 8080:8080
    depends_on:
      - redpanda-0
```
Answer: This is a Docker Compose file for a **single-broker Redpanda quickstart** setup. Here's a breakdown of each section: [[Quickstart Deploy](https://docs.redpanda.com/streaming/24.2/get-started/quick-start/#deploy-redpanda)]

---

## Top-level structure

```yaml
name: redpanda-quickstart-one-broker
```
Names the Docker Compose project.

```yaml
networks:
  redpanda_network:
    driver: bridge
```
Creates a private bridge network so containers can communicate with each other by service name.

```yaml
volumes:
  redpanda-0: null
```
Declares a named Docker volume to persist Redpanda's data across container restarts.

---

## `redpanda-0` service (the broker)

```yaml
image: docker.redpanda.com/redpandadata/redpanda:v26.1.13
container_name: redpanda-0
```
Pulls and runs Redpanda version 26.1.13.

### Key startup flags:

| Flag | Purpose |
|---|---|
| `--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092` | Binds the Kafka API on two listeners: one for internal Docker traffic (port 9092) and one for external clients (port 19092). |
| `--advertise-kafka-addr internal://redpanda-0:9092,external://localhost:19092` | Tells clients which address to use — internal clients use the Docker hostname `redpanda-0:9092`; external clients use `localhost:19092`. |
| `--pandaproxy-addr` / `--advertise-pandaproxy-addr` | Same dual-listener pattern for the **HTTP Proxy** (Pandaproxy) on ports 8082 / 18082. |
| `--schema-registry-addr` | Binds the Schema Registry on ports 8081 / 18081. |
| `--rpc-addr` / `--advertise-rpc-addr` | The internal RPC address brokers use to communicate with each other (port 33145). |
| `--mode dev-container` | Applies preset configuration suitable for development/testing (e.g., disables production guards, enables auto topic creation). |
| `--smp 1` | Limits Redpanda's underlying Seastar framework to use only **1 CPU core**. |
| `--default-log-level=info` | Sets log verbosity to `info`. |

### Volumes & ports:

```yaml
volumes:
  - redpanda-0:/var/lib/redpanda/data   # Persists broker data
ports:
  - 18081:18081   # Schema Registry (external)
  - 18082:18082   # HTTP Proxy (external)
  - 19092:19092   # Kafka API (external)
  - 19644:9644    # Admin API (external port 19644 → internal 9644)
```

---

## `console` service (Redpanda Console UI)

```yaml
image: docker.redpanda.com/redpandadata/console:v3.8.0
container_name: redpanda-console
depends_on:
  - redpanda-0
```
Runs the Redpanda Console web UI and waits for the broker to start first.

```yaml
command: -c 'echo "$$CONSOLE_CONFIG_FILE" > /tmp/config.yml; /app/console'
```
Writes the configuration from the environment variable into a file, then starts the Console app.

### Console configuration (via `CONSOLE_CONFIG_FILE`):

```yaml
kafka:
  brokers: ["redpanda-0:9092"]        # Connects to the broker internally
schemaRegistry:
  enabled: true
  urls: ["http://redpanda-0:8081"]    # Schema Registry endpoint
redpanda:
  adminApi:
    enabled: true
    urls: ["http://redpanda-0:9644"]  # Admin API endpoint
```

```yaml
ports:
  - 8080:8080   # Console UI accessible at http://localhost:8080
```

---

## Summary

This setup spins up **one Redpanda broker** and the **Redpanda Console UI** in Docker. It's designed for local development and exploration — not production use. Once running, you can access the Console at `http://localhost:8080` and connect Kafka clients to `localhost:19092`. [[Quickstart Deploy](https://docs.redpanda.com/streaming/24.2/get-started/quick-start/#deploy-redpanda)]