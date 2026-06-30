# pip install google-cloud-pubsub

import json
import random
import time
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "p101-473210"
TOPIC_ID   = "gaming-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

games = ["poker", "slots", "trivia", "racing"]
users = [f"user_{i:04d}" for i in range(1, 501)]
event_types = ["play_round", "wager", "login"]

def generate_event():
    etype = random.choice(event_types)
    # each play_round has a stake and score_delta
    score = random.randint(-20, 100) if etype == "play_round" else 0
    wager = round(random.random() * 2000, 2) if etype in ("play_round", "wager") else 0.0
    return {
        "user_id":      random.choice(users),
        "game_id":      random.choice(games),
        "event_type":   etype,
        "score_delta":  score,
        "wager_amount": wager,
        "ts":           datetime.utcnow().isoformat() + "Z"
    }

if __name__ == "__main__":
    print(f"Publishing to Pub/Sub topic: {topic_path}")
    while True:
        event = generate_event()
        data = json.dumps(event).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result()
        print("Published:", event)
        time.sleep(2)