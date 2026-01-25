# Tanako Bot – Production & Scaling Guide

Tanako Bot is a horizontally scalable for security and utilities
Discord bot designed to operate reliably across thousands of guilds.

It uses:
- Sharded Discord gateway connections
- Multiple bot instances
- A distributed Database API layer
- Consistent hashing based on `guild_id`
- Horizontal scaling at both bot and database layers

This document describes how to deploy and scale Tanako Bot in production environments.

---

# Architecture Overview

Tanako Bot uses a distributed multi-layer architecture:

                    ┌─────────────────┐
                    │   Discord API   │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
     ┌─────▼─────┐     ┌─────▼─────┐     ┌─────▼─────┐
     │ Bot Shard │     │ Bot Shard │     │ Bot Shard │
     │   0, 1    │     │   2, 3    │     │   4, 5    │
     └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
           │                 │                 │
           └─────────────────┼─────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Load Balancer  │
                    │  (WebSocket)    │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
     ┌─────▼─────┐     ┌─────▼─────┐     ┌─────▼─────┐
     │  DB Node  │◄───►│  DB Node  │◄───►│  DB Node  │
     │     1     │     │     2     │     │     3     │
     └───────────┘     └───────────┘     └───────────┘

Data Flow:
Discord API → Bot Shards → WebSocket Load Balancer → Database API Cluster

Each guild is mapped to a primary database node using consistent hashing.
Other nodes may cache data with TTL.

---

# 1. Database API Server

## Single Node (Development)

Run locally:

python database_api/server.py

Environment variables:

DB_API_HOST=0.0.0.0
DB_API_PORT=8080
DB_DATA_DIR=data/db
DB_CLUSTER_ENABLED=false

---

## Cluster Mode (Production)

Cluster mode distributes guild data across multiple nodes.

### Node 1 (Seed Node)

DB_CLUSTER_ENABLED=true
DB_NODE_ID=node1
DB_API_HOST=0.0.0.0
DB_API_PORT=8080
python database_api/server.py

### Node 2

DB_CLUSTER_ENABLED=true
DB_NODE_ID=node2
DB_API_HOST=0.0.0.0
DB_API_PORT=8080
DB_SEED_NODES=node1-ip:8080
python database_api/server.py

### Node 3

DB_CLUSTER_ENABLED=true
DB_NODE_ID=node3
DB_API_HOST=0.0.0.0
DB_API_PORT=8080
DB_SEED_NODES=node1-ip:8080,node2-ip:8080
python database_api/server.py

---

## Database Scaling Strategy

Guild Count         Recommended DB Nodes
< 1,000             1 node
1,000 – 10,000      2–3 nodes
10,000 – 50,000     3–5 nodes
> 50,000            5+ nodes

---

# Load Balancer (HAProxy Example)

frontend websocket_frontend
    bind *:8080
    mode tcp
    default_backend db_nodes

backend db_nodes
    mode tcp
    balance roundrobin
    server node1 192.168.1.10:8080 check
    server node2 192.168.1.11:8080 check
    server node3 192.168.1.12:8080 check

---

# 2. Discord Bot Sharding

Discord requires sharding when bots exceed 2,500 guilds.

## Single Instance (< 2,500 guilds)

python main.py

## Multi-Shard Single Process

SHARD_COUNT=4 python main.py

Runs all shards inside one process.

## Multi-Instance Sharding (Large Bots)

Instance 1 (Shards 0–1)

SHARD_COUNT=4
SHARD_IDS=0,1
DB_API_URI=ws://db-loadbalancer:8080
python main.py

Instance 2 (Shards 2–3)

SHARD_COUNT=4
SHARD_IDS=2,3
DB_API_URI=ws://db-loadbalancer:8080
python main.py

---

## Sharding Scaling Strategy

Guild Count         Shards      Instances
< 2,500             1           1
2,500 – 10,000      2–4         1–2
10,000 – 50,000     5–20        2–5
> 50,000            20+         5+

---

# 3. Docker Deployment

Example docker-compose.yml:

version: '3.8'

services:
  db-node1:
    build: .
    command: python database_api/server.py
    environment:
      - DB_CLUSTER_ENABLED=true
      - DB_NODE_ID=node1
    volumes:
      - db-data-1:/app/data

  db-node2:
    build: .
    command: python database_api/server.py
    environment:
      - DB_CLUSTER_ENABLED=true
      - DB_NODE_ID=node2
      - DB_SEED_NODES=db-node1:8080
    depends_on:
      - db-node1
    volumes:
      - db-data-2:/app/data

  db-node3:
    build: .
    command: python database_api/server.py
    environment:
      - DB_CLUSTER_ENABLED=true
      - DB_NODE_ID=node3
      - DB_SEED_NODES=db-node1:8080,db-node2:8080
    depends_on:
      - db-node1
      - db-node2
    volumes:
      - db-data-3:/app/data

  bot-1:
    build: .
    command: python main.py
    environment:
      - DISCORD_TOKEN=${DISCORD_TOKEN}
      - DB_API_URI=ws://db-loadbalancer:8080
      - SHARD_COUNT=4
      - SHARD_IDS=0,1

  bot-2:
    build: .
    command: python main.py
    environment:
      - DISCORD_TOKEN=${DISCORD_TOKEN}
      - DB_API_URI=ws://db-loadbalancer:8080
      - SHARD_COUNT=4
      - SHARD_IDS=2,3

volumes:
  db-data-1:
  db-data-2:
  db-data-3:

---

# 4. Kubernetes Deployment

## Database StatefulSet

apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tanako-db
spec:
  serviceName: tanako-db
  replicas: 3
  selector:
    matchLabels:
      app: tanako-db
  template:
    metadata:
      labels:
        app: tanako-db
    spec:
      containers:
      - name: db
        image: tanako-bot:latest
        command: ["python", "database_api/server.py"]
        env:
        - name: DB_CLUSTER_ENABLED
          value: "true"
        - name: DB_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 8080

---

# 5. Monitoring

Health Check:

GET /health

Returns "OK" if the Database API is running.

Metrics include:

- CPU / Memory usage
- Guild count per shard
- Latency
- Uptime

Bot command:

/metrics (currently cannot use)

---

# 6. Backup & Recovery

Backup:

cp -r data/db data/db_backup_YYYYMMDD

Recovery:

1. Stop all DB nodes
2. Restore backup into data directory
3. Restart nodes

---

# Production Checklist

[ ] Store DISCORD_TOKEN securely  
[ ] Restrict DB ports (internal network only)  
[ ] Enable SSL/TLS for WebSocket connections  
[ ] Configure persistent storage  
[ ] Enable health checks and auto-restart  
[ ] Configure centralized logging  
[ ] Test failover scenarios  
[ ] Implement scheduled backups  
