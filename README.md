# 📖 Distributed Systems
### Table of Contents


| Component                             | Description                                                                                                                                                                   |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Key Value Store** | Distributed, durable, in-memory key-value database with replication, leader election, and snapshotting. Includes: metadata server (RAFT), storage server, dashboard, and client library. |
| **2. Online Store**                | A basic, mock e-commerce system built for testing, with its components (product service, order service, ordering UI, and orders dashboard) storing data in the key value store.                        |
| **3. Config Server**        | Centralized configuration management service for all components.                               |
| **4. Api Gateway**        | A gateway which api calls from the ordering UI of the online store pass through. Has a rate limiter and light security.                                           |
| **5. Consistent Hashing Layer**    | Implements a ring-based consistent hashing algorithm used in the KVS and rate limiter.                                                                 |
| **6. Deployment Automation**       | Scripts and tooling to automate deployment, scaling, and monitoring of the distributed system components across environments. Has kubernetes and docker compose versions.                                                 |



## **1. Key Value Store**
*A distributed, durable, in-memory key-value store built from scratch.*


| ✅ Module            | Description                                                                                                                                                                             |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Metadata Server** | Coordinates metadata and performs RAFT-based leader election and replication.                                                                                                           |
| **Storage Server**  | Handles in-memory key-value storage, with leader-follower replication using gRPC or REST (configurable).                                                                                |
| **Dashboard**       | Provides real-time visibility into the cluster’s health, metadata state, storage nodes, partitions, replicas, items, and item versions. Supports admin operations like creating tables. |
| **Client Library**  | A lightweight library to interact with the system via API calls.                                                                                                                        |

<br/>

⚙️ Key Features

✔ RAFT-based Leader Election (Metadata Server)

✔ Custom Leader Election Algorithm (Storage Servers)

✔ Leader-Follower Replication (via gRPC or REST)

✔ Write-Ahead Logging (WAL) for durability

✔ Snapshotting for fast recovery and reduced replay times

✔ Data Compression to optimize memory usage

✔ Time-To-Live (TTL) for expiring entries

✔ Secondary Indexes for efficient querying

✔ Cursor-based Pagination for scalable data access


<br>
📦 Tech Stack

Language:    Java

Communication: gRPC, REST

Consensus: RAFT


<br/>
📄 License: This project is licensed under the MIT License.
