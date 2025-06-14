# MIT6.5840: Highly-Available Sharded Key/Value Storage System
This project is the assignment for MIT 6.5840 (Distributed Systems) course, Spring 2025 semester.
## Project Overview
This project implements a **highly-available, scalable, and fault-tolerant sharded key/value storage service**. The system leverages the **Raft consensus protocol** to ensure data consistency and fault tolerance, and achieves horizontal scalability through the use of **multiple shard groups**. Furthermore, it supports **dynamic reconfiguration** to handle changes in load. Each key-value pair is **versioned**, reflecting the inherent trade-offs described by the **CAP theorem**.
## All Tests Passed
**I have successfully passed all the tests for the MIT 6.5840 course, demonstrating the correctness, high availability, and fault tolerance of this distributed key-value system.**
## Technology Stack
- Go Language
- RPC
- Raft Protocol