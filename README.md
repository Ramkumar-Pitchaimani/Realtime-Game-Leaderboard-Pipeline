# 🎮 Real-Time Gaming Leaderboard Analytics Platform on Google Cloud

## Overview

The **Real-Time Gaming Leaderboard Analytics Platform** is an event-driven streaming analytics solution built on **Google Cloud Platform (GCP)**.

The platform continuously ingests gaming events, processes them in real time using Google Cloud Dataflow, updates player scores, detects leaderboard milestones, triggers reward notifications, and stores analytical data in BigQuery for reporting and dashboards.

The solution demonstrates enterprise-scale stream processing, event-driven architecture, and real-time analytics using modern Google Cloud services.

---

# Business Problem

Online multiplayer games generate millions of player events every hour.

Gaming companies require a highly scalable platform capable of:

- Processing player score events in real time
- Updating live leaderboards
- Detecting milestone achievements
- Triggering rewards instantly
- Maintaining historical player statistics
- Supporting analytical dashboards

Traditional batch processing cannot provide the low-latency experience required for modern online games.

This project demonstrates a cloud-native real-time leaderboard analytics solution.

---

# Solution Architecture

```

                    Mock Gaming Event Generator
                               │
                               ▼
                     Google Pub/Sub Topic
                       gaming-events
                               │
                               ▼
                  Google Cloud Dataflow Pipeline
        ------------------------------------------------
        • Read Pub/Sub Messages
        • Parse JSON
        • Validate Events
        • Calculate Player Score
        • Update Leaderboard
        • Write Current Scores
        • Publish Reward Events
        ------------------------------------------------
                   │                       │
                   ▼                       ▼
             Google BigQuery        Google Pub/Sub
            player_events         reward-events
                   │                       │
                   ▼                       ▼
          Scheduled BigQuery Job    Cloud Run Function
          ----------------------    ------------------------
          • Refresh Leaderboard     • Consume Reward Events
          • Rank Players            • Check Reward Threshold
          • Compute Top N           • Send Reward Notification
          • Materialized View       • Store Reward History
          ----------------------    ------------------------
                   │
                   ▼
            BigQuery Leaderboard
             Analytics & Reports

```

---

# Technology Stack

| Layer | Technology |
|--------|------------|
| Cloud Platform | Google Cloud Platform |
| Streaming Platform | Google Pub/Sub |
| Stream Processing | Google Cloud Dataflow |
| Processing Framework | Apache Beam |
| Compute | Cloud Run Functions |
| Data Warehouse | BigQuery |
| Programming Language | Python |
| Analytics | SQL / Looker Studio |

---

# Project Workflow

## Step 1 – Generate Gaming Events

A mock gaming event generator continuously creates player activity.

Example Event

```json
{
  "player_id":"PLAYER1001",
  "game_id":"GAME101",
  "score":350,
  "coins":120,
  "event_time":"2026-07-10T15:20:00Z"
}
```

The events are published to Pub/Sub.

Topic

```
gaming-events
```

---

## Step 2 – Real-Time Stream Processing

Google Cloud Dataflow continuously processes incoming events.

Responsibilities:

- Read Pub/Sub messages
- Parse JSON
- Validate records
- Remove duplicate events
- Aggregate player scores
- Calculate running totals
- Write events into BigQuery

---

## Step 3 – Store Player Events

Every validated event is stored in BigQuery.

Dataset

```
gaming_dw
```

Table

```
player_events
```

Historical event data supports reporting and player analytics.

---

## Step 4 – Publish Reward Events

Whenever a player reaches a predefined score threshold, Dataflow publishes a reward event.

Topic

```
reward-events
```

Example

```json
{
  "player_id":"PLAYER1001",
  "score":5000,
  "reward":"Gold Badge"
}
```

---

## Step 5 – Reward Processing

Cloud Run Function subscribes to the reward-events topic.

Responsibilities:

- Receive reward events
- Validate reward threshold
- Send player notification
- Log reward history
- Persist reward information into BigQuery

---

## Step 6 – Refresh Leaderboard

A scheduled BigQuery query executes every minute.

Responsibilities:

- Rank players
- Compute Top 10 / Top 100
- Refresh leaderboard table
- Create reporting views

---

# Dataflow Processing Logic

The streaming pipeline performs:

- Read Pub/Sub events
- Parse JSON payload
- Validate schema
- Calculate cumulative player score
- Maintain running totals
- Write player events to BigQuery
- Publish reward events

---

# Pub/Sub Topics

| Topic | Purpose |
|--------|---------|
| gaming-events | Incoming player activity |
| reward-events | Reward milestone notifications |

---

# BigQuery Tables

## player_events

| Column | Type |
|---------|------|
| player_id | STRING |
| game_id | STRING |
| score | INTEGER |
| coins | INTEGER |
| level | INTEGER |
| country | STRING |
| event_time | TIMESTAMP |

---

## leaderboard

| Column | Type |
|---------|------|
| rank | INTEGER |
| player_id | STRING |
| total_score | INTEGER |
| last_updated | TIMESTAMP |

---

## reward_history

| Column | Type |
|---------|------|
| reward_id | STRING |
| player_id | STRING |
| reward_name | STRING |
| awarded_at | TIMESTAMP |

---

# Sample SQL Queries

## Top 10 Players

```sql
SELECT
player_id,
SUM(score) AS total_score
FROM gaming_dw.player_events
GROUP BY player_id
ORDER BY total_score DESC
LIMIT 10;
```

---

## Country-wise Leaderboard

```sql
SELECT
country,
SUM(score) AS total_score
FROM gaming_dw.player_events
GROUP BY country
ORDER BY total_score DESC;
```

---

## Reward Distribution

```sql
SELECT
reward_name,
COUNT(*) AS total_rewards
FROM gaming_dw.reward_history
GROUP BY reward_name;
```

---

# Repository Structure

```
realtime-gaming-leaderboard-analysis/

│
├── dataflow/
│      leaderboard_pipeline.py
│
├── cloud-run/
│      reward_processor.py
│
├── pubsub/
│      gaming_event_publisher.py
│
├── sql/
│      leaderboard_refresh.sql
│
├── architecture/
│      architecture.png
│
├── deployment/
│      deployment-guide.png
│
├── sample_data/
│
├── screenshots/
│
├── README.md
```

---

# Features

- Real-Time Event Streaming
- Live Leaderboard Calculation
- Event-Driven Architecture
- Pub/Sub Messaging
- Apache Beam Dataflow Pipeline
- Reward Notification Engine
- Cloud Run Processing
- Scheduled BigQuery Leaderboard Refresh
- BigQuery Analytics
- Enterprise Streaming Architecture

---

# Skills 

- Google Cloud Platform
- Pub/Sub
- Dataflow
- Apache Beam
- Cloud Run
- BigQuery
- Python
- Event-Driven Architecture
- Real-Time Analytics
- Window Aggregations
- Leaderboard Computation
- Streaming ETL

---

