# Raft Key‑Value Server

A fault‑tolerant distributed key‑value store built using the **Raft consensus algorithm**.  
Ideal for educational purposes and experimentation with leader election, replicated logs, and state machine replication.


## Features

- Strong consensus via leader election and log replication  
- Supports basic operations: `GET`, `SET`, and `CAS` (compare-and‑swap)  
- Fault-tolerant: operates when a majority of nodes are available  
- HTTP API for external client interactions
  

## Architecture Overview

1. **Raft Module**  
   - Manages leader election, log replication, and safety guarantees like log matching and leader completeness

2. **State Machine / KV Store**  
   - A simple in-memory Go `map[string]string` as the key‑value store  
   - Commands (`GET`, `SET`, `CAS`) are serialized and applied upon consensus via the Raft log

3. **HTTP API Layer**  
   - Accepts client requests, encodes operations into Raft commands, and responds after commit  
   - Provides endpoints like `/set?key=...&value=...` and `/get?key=...`


## Getting Started

### Prerequisites

- **Go 1.20+**  
- Network access between Raft nodes


### Setup Instructions

```bash
git clone https://github.com/avishek04/Raft-Key-Value-Server.git
cd Raft-Key-Value-Server
go mod tidy
```

### Running a Local Cluster (3 Nodes)

```bash
# Node 1
go run cmd/kvapi/main.go --port=2020 --dir=data1 &

# Node 2
go run cmd/kvapi/main.go --port=2021 --dir=data2 &

# Node 3
go run cmd/kvapi/main.go --port=2022 --dir=data3 &
```

### Example Usage

```bash
# Write a key‑value pair
curl "http://localhost:2020/set?key=x&value=42"

# Read a key
curl "http://localhost:2020/get?key=x"
```

## Testing

Test scenarios should include:

- Leader failover  
- Node partition and recovery  
- Concurrent client requests


## Consistency & Safety Guarantees

Raft ensures:

- **Election Safety**: Only one leader per term  
- **Log Matching**: Entries with same index and term are identical  
- **Leader Completeness**: Committed entries will persist in future terms  
- **State Machine Safety**: No conflicting applied commands


## Configuration Options

Configuration parameters you can adjust include:

- RPC timeouts and heartbeat intervals  
- Snapshotting / log compaction (if implemented)  
- Size and persistence of state logs  
- Cluster membership and dynamic reconfiguration


## Contributing

Contributions welcome! Here’s how to help:

- Report or fix bugs  
- Add support for snapshotting or persistent storage  
- Implement client authentication or TLS encryption  
- Containerize or provide deployment scripts  
- Write benchmarks or stress tests
  

## License

Distributed under the **MIT License**.


## Further Reading

- **In Search of an Understandable Consensus Algorithm** by Ongaro & Ousterhout – the original Raft paper ([raft.github.io](https://raft.github.io/raft.pdf))  
- Tutorials and deep dives into Raft, including building a key‑value store in Go ([eli.thegreenplace.net](https://eli.thegreenplace.net/2024/implementing-raft-part-4-keyvalue-database/))
