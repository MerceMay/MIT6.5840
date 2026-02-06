# MIT6.5840: Highly-Available Sharded Key/Value Storage System

This project is the assignment for MIT 6.5840 (Distributed Systems) course, Spring 2025 semester.

## Project Overview

This project implements a **highly-available, scalable, and fault-tolerant sharded key/value storage service**. The system leverages the **Raft consensus protocol** to ensure data consistency and fault tolerance, and achieves horizontal scalability through the use of **multiple shard groups**. Furthermore, it supports **dynamic reconfiguration** to handle changes in load. Each key-value pair is **versioned**, reflecting the inherent trade-offs described by the **CAP theorem**.

## All Tests Passed

**I have successfully passed all the tests for the MIT 6.5840 course, demonstrating the correctness, high availability, and fault tolerance of this distributed key-value system.**

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                     Client Layer                     │
│  (Clerk: leader tracking, retry, ErrMaybe semantics) │
├──────────────────────────────────────────────────────┤
│                  ShardKV Controller                  │
│  (Freeze→Install→Delete shard migration protocol)    │
├──────────────────────────────────────────────────────┤
│             Replicated State Machine (RSM)            │
│     (PendingOps tracking, async snapshot, Submit)     │
├──────────────────────────────────────────────────────┤
│                  Raft Consensus Core                  │
│  (Leader election, log replication, log compaction)   │
├──────────────────────────────────────────────────────┤
│                   Persistence Layer                   │
│         (Persister: Raft state + snapshots)           │
└──────────────────────────────────────────────────────┘
```

## Key Technical Highlights

- **ConflictTerm Fast Backtracking**: Reduces log convergence RPCs from O(N) to O(T) after network partition recovery (T = number of conflicting terms)
- **Channel-Driven Async Event Architecture**: Decouples timers, heartbeats, and elections via buffered channels with non-blocking writes
- **Per-Key RWMutex Lock Separation**: Fine-grained read-write locks enable concurrent reads on the same key and full parallelism across different keys
- **Freeze→Install→Delete Shard Migration**: Three-phase protocol with version-based idempotency for zero-data-loss online shard migration
- **Async Snapshot with 90% Threshold**: Non-blocking snapshot creation via goroutines with proactive triggering before log limit

> For a detailed technical analysis, see [TECHNICAL_ANALYSIS.md](TECHNICAL_ANALYSIS.md).

## Technology Stack

- Go Language
- RPC (labrpc)
- Raft Consensus Protocol
- Porcupine (Linearizability Checker)