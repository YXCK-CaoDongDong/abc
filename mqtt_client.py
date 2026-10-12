#!/usr/bin/env python3
"""Simple MQTT client: connect to public EMQX broker, subscribe, publish, report result."""

from __future__ import annotations

import argparse
import socket
import sys
import time
import uuid

import paho.mqtt.client as mqtt

BROKER_HOST = "broker.emqx.io"
BROKER_PORT = 1883
DEFAULT_TOPIC = "cursor/mqtt-test/demo"
DEFAULT_MESSAGE = "hello from mqtt client"


def main() -> int:
    parser = argparse.ArgumentParser(description="Test MQTT connect/subscribe/publish to EMQX public broker")
    parser.add_argument("--host", default=BROKER_HOST)
    parser.add_argument("--port", type=int, default=BROKER_PORT)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--message", default=DEFAULT_MESSAGE)
    parser.add_argument("--timeout", type=float, default=15.0, help="Seconds to wait for connect/round-trip")
    args = parser.parse_args()

    client_id = f"cursor-mqtt-{uuid.uuid4().hex[:12]}"
    state = {
        "connected": False,
        "subscribed": False,
        "published": False,
        "received": False,
        "error": None,
        "rc": None,
    }

    def _rc_value(reason_code) -> int:
        if reason_code is None:
            return -1
        try:
            return int(reason_code)
        except (TypeError, ValueError):
            value = getattr(reason_code, "value", None)
            return int(value) if value is not None else -1

    def on_connect(client, userdata, flags, reason_code, properties=None):
        # paho v2 uses ReasonCode; treat 0 / Success as connected
        rc = _rc_value(reason_code)
        state["rc"] = rc
        is_success = rc == 0 or str(reason_code) in ("Success", "0")
        if is_success:
            state["connected"] = True
            print(f"[OK] Connected to {args.host}:{args.port} (client_id={client_id})")
            client.subscribe(args.topic, qos=0)
        else:
            state["error"] = f"CONNACK refused, reason_code={reason_code!s} (rc={rc})"
            print(f"[FAIL] Connect refused: {state['error']}")

    def on_connect_fail(client, userdata):
        state["error"] = "on_connect_fail: TCP/TLS connect failed before CONNACK"
        print(f"[FAIL] {state['error']}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
        rc = _rc_value(reason_code)
        if not state["connected"] and state["error"] is None:
            state["error"] = f"Disconnected before successful session (reason_code={reason_code!s}, rc={rc})"
            print(f"[FAIL] {state['error']}")

    def on_subscribe(client, userdata, mid, reason_codes, properties=None):
        state["subscribed"] = True
        print(f"[OK] Subscribed to topic: {args.topic}")
        info = client.publish(args.topic, args.message, qos=0)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            state["published"] = True
            print(f"[OK] Published message: {args.message!r}")
        else:
            state["error"] = f"Publish failed: rc={info.rc}"
            print(f"[FAIL] {state['error']}")

    def on_message(client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        print(f"[OK] Received on {msg.topic}: {payload!r}")
        if payload == args.message:
            state["received"] = True

    def on_log(client, userdata, level, buf):
        # Keep quiet unless troubleshooting; uncomment for verbose:
        # print(f"[LOG] {buf}")
        pass

    print(f"[INFO] Connecting to mqtt://{args.host}:{args.port} (no TLS, no auth)...")
    print(f"[INFO] client_id={client_id}")
    print(f"[INFO] topic={args.topic}")

    # Preflight TCP reachability for clearer errors
    try:
        with socket.create_connection((args.host, args.port), timeout=min(5.0, args.timeout)) as sock:
            print(f"[OK] TCP reachability to {args.host}:{args.port} succeeded")
    except OSError as exc:
        print(f"[FAIL] TCP connect to {args.host}:{args.port} failed: {exc}")
        print("[FAIL] MQTT connect did not succeed")
        return 1

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )
    client.on_connect = on_connect
    client.on_connect_fail = on_connect_fail
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_log = on_log

    try:
        client.connect(args.host, args.port, keepalive=60)
    except Exception as exc:  # noqa: BLE001 - report any connect error clearly
        print(f"[FAIL] client.connect() raised: {type(exc).__name__}: {exc}")
        print("[FAIL] MQTT connect did not succeed")
        return 1

    client.loop_start()
    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline:
            if state["error"] and not state["connected"]:
                break
            if state["connected"] and state["subscribed"] and state["published"] and state["received"]:
                break
            time.sleep(0.1)
    finally:
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass

    if state["connected"] and state["subscribed"] and state["published"] and state["received"]:
        print("[SUCCESS] Connect, subscribe, publish, and receive all succeeded")
        return 0

    if state["connected"]:
        missing = []
        if not state["subscribed"]:
            missing.append("subscribe")
        if not state["published"]:
            missing.append("publish")
        if not state["received"]:
            missing.append("receive")
        print(f"[PARTIAL] Connected, but incomplete: missing={','.join(missing)}")
        if state["error"]:
            print(f"[FAIL] Error detail: {state['error']}")
        return 2

    err = state["error"] or "timed out waiting for CONNACK"
    print(f"[FAIL] MQTT connect did not succeed: {err}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
