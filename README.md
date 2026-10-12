# MQTT client demo

Small Python MQTT client that connects to a broker, subscribes to a topic, publishes a short message, and prints clear success/failure logs.

## Setup

```bash
pip3 install -r requirements.txt
```

## Run

```bash
python3 mqtt_client.py
```

Defaults: `broker.emqx.io:1883` (MQTT, no TLS, no auth).

### Broker options

```bash
# default public EMQX broker
python3 mqtt_client.py --host broker.emqx.io --port 1883

# fallback public broker (verified working during testing)
python3 mqtt_client.py --host broker.hivemq.com --port 1883
```

Other useful flags: `--topic`, `--message`, `--timeout`.

### Note on public brokers

During testing from this environment, **`broker.emqx.io:1883` failed** (TCP connected, but the MQTT session was reset / no successful CONNACK). The same client worked against **`broker.hivemq.com:1883`** as a fallback. Prefer HiveMQ if EMQX is unreachable.
