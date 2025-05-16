# 📖 Distributed Systems

## Table of Contents

| Component                  | Description                                                                                                                                            |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| **1. Key Value Store**      | Distributed, durable, in-memory key-value database with replication, leader election, and snapshotting. Includes: metadata server (RAFT), storage server, dashboard, and client library. |
| **2. Online Store**         | A mock e-commerce system built for testing, with product service, order service, ordering UI, and orders dashboard storing data in the key value store. |
| **3. Config Server**        | Centralized configuration management service for all components. Reads config data from another GitHub repo (config-repo).                             |
| **4. API Gateway**          | Gateway through which API calls from the ordering UI pass. Includes rate limiting and light security features.                                          |
| **5. Consistent Hashing Layer** | Implements a ring-based consistent hashing algorithm used in the KVS and rate limiter.                                                                  |
| **6. Deployment Automation**| Scripts to automate deployment, including Kubernetes (Helm) and Docker Compose versions.                                                                |

---

## 1. Key Value Store

*A distributed, durable, in-memory key-value store built from scratch.*

| Module             | Description                                                                                                                                                   |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Metadata Server** | Coordinates metadata (storage nodes, tables, partitions, partition leaders, etc.) and performs RAFT-based leader election and replication.                   |
| **Storage Server**  | Handles in-memory data storage with leader-follower replication via gRPC or REST (configurable). The leader is elected by the active metadata server.         |
| **Dashboard**       | Provides real-time visibility into cluster health, metadata (storage nodes, tables, partitions, replicas), items, and item versions. Supports admin operations like creating tables. |
| **Client Library**  | Lightweight library for client applications to interact with the key value store via REST API calls.                                                          |

---

### Architectural Inspiration

> The replication architecture of the storage server is partly inspired by the Apache Kafka design.  
> This implementation is entirely independent and not affiliated with the Apache Kafka project.

---

### ⚙️ Key Features

- RAFT-based Leader Election (Metadata Server)  
- Custom Leader Election Algorithm (Storage Servers)  
- Leader-Follower Replication (via gRPC or REST)  
- Write-Ahead Logging (WAL) for durability  
- Snapshotting for fast recovery and reduced replay times  
- Data Compression to optimize memory usage  
- Time-To-Live (TTL) for expiring entries  
- Secondary Indexes for efficient querying  
- Cursor-based Pagination for scalable data access  

---

### 📦 Tech Stack

- **Language:** Java  
- **Communication:** gRPC, REST  
- **Consensus:** RAFT  

---

### 📄 License

This project is licensed under the **MIT License**.  
