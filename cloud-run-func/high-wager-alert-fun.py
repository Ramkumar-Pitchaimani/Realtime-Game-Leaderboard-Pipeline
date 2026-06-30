import os
import json
import base64
import logging
import functions_framework

# Threshold can be overridden via an env var in Cloud Run
WAGER_THRESHOLD = 1000

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    """Triggered from a message on a Cloud Pub/Sub topic."""
    # 1) Decode the Pub/Sub “data” field
    data_b64 = cloud_event.data["message"].get("data")
    if not data_b64:
        logging.error("No data in Pub/Sub message")
        return

    try:
        payload = base64.b64decode(data_b64).decode("utf-8")
        record = json.loads(payload)
    except Exception as e:
        logging.error(f"Failed to decode/parse JSON: {e} — payload={data_b64}")
        return

    # 2) Check for a wager event
    if record.get("event_type") != "wager":
        logging.info(f"Ignoring non-wager event: {record.get('event_type')}")
        return

    # 3) Validate and threshold
    try:
        amt = float(record.get("wager_amount", 0))
    except (TypeError, ValueError):
        logging.error(f"Invalid wager_amount: {record.get('wager_amount')}")
        return

    if amt > WAGER_THRESHOLD:
        logging.warning(
            f"🚨 HIGH WAGER ALERT: user={record.get('user_id')} "
            f"game={record.get('game_id')} amount={amt}"
        )
    else:
        logging.info(f"Wager OK: user={record.get('user_id')} amount={amt}")