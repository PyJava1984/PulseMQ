# PulseMQ
 A high-performance, distributed message broker designed for Java, Spring Boot, Go, Python, Rust, and other applications.

 PulseMQ is a high-throughput messaging platform being built for applications that require reliable, scalable, low-latency asynchronous communication.

 The broker is written in Rust and is designed around:

 - Partitioned message streams
- Append-only persistent logs
- Batch-oriented I/O
- High-throughput binary networking
- Consumer groups
- Backpressure
- Horizontal scalability
- Replication and fault tolerance
- Multi-language client SDKs
- Spring Boot integration

 The long-term architectural target is millions of messages per second across a broker cluster, depending on message size, hardware, durability mode, replication factor, workload, and configuration.

 > **Project status:** Early development. APIs, protocol formats, storage formats, and cluster semantics are subject to change.

 ## Table of Contents

 - Vision
- Goals
- Non-Goals
- Architecture
- Core Design Principles
- Features
- Message Model
- Topics and Partitions
- Producer Model
- Consumer Model
- Consumer Groups
- Delivery Semantics
- Persistence
- Replication
- Backpressure
- Protocol
- Client SDKs
- Spring Boot Integration
- Security
- Observability
- Configuration
- Repository Structure
- Technology Stack
- Performance Strategy
- Benchmarking
- Reliability
- Failure Handling
- Deployment
- Development Setup
- Running the Broker
- Example Producer
- Example Consumer
- Roadmap
- Versioning
- Compatibility
- Testing Strategy
- Performance Acceptance Criteria
- Contribution Guidelines
- Architecture Decision Records
- Security Policy
- License
- Disclaimer

---

 ## Vision

 The goal is to build a modern messaging platform suitable for:

```
                    Applications
                         │
          ┌──────────────┼──────────────┐
          │              │              │
        Java            Go           Python
          │              │              │
        Rust          Node.js         C#
          │              │              │
          └──────────────┼──────────────┘
                         │
                         ▼
                 ┌───────────────┐
                 │   MQ Broker   │
                 └───────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
      Partition 0    Partition 1    Partition N
          │              │              │
          ▼              ▼              ▼
         WAL            WAL            WAL
          │              │              │
          └──────────────┼──────────────┘
                         ▼
                        NVMe
```

 The platform is intended to support both:

 - traditional queue-based workloads
- high-throughput event-streaming workloads

 The architecture favors horizontal scaling through partitions and brokers rather than relying on a single extremely powerful queue.

 ## Goals

 ### Primary Goals

 #### 1\. High throughput

 The architecture should scale toward:

```
1M+
messages/sec
```

 and eventually:

```
10M+
messages/sec
```

 across appropriately provisioned broker clusters.

 These are architectural targets, not current benchmark claims.

 Actual performance will depend on:

 - message size
- batch size
- producer count
- consumer count
- partition count
- CPU
- RAM
- network bandwidth
- storage
- replication factor
- durability settings
- compression
- workload pattern

 #### 2\. Low predictable latency

 The system should target predictable:

 - p50
- p95
- p99
- p99.9

 latency rather than optimizing only average latency.

 #### 3\. Horizontal scalability

 Performance should increase by adding:

```
partitions
+
broker nodes
+
consumer instances
```

 rather than depending on a single global queue.

 #### 4\. Durability

 Messages should be able to survive:

 - process restart
- machine restart
- broker failure
- disk recovery

 depending on configured durability and replication guarantees.

 #### 5\. Multi-language support

 Official client SDKs are planned for:

 - Java
- Go
- Python
- Rust

 Additional languages can implement the public wire protocol.

 #### 6\. Developer experience

 The system should be easy to integrate with:

 - Spring Boot
- microservices
- REST applications
- background workers
- event-driven architectures
- data-processing pipelines

 ## Non-Goals

 The project will not attempt to become everything at once.

 Initially, the project will not prioritize:

 - SQL querying
- full event-stream processing
- distributed databases
- workflow orchestration
- serverless execution
- arbitrary message transformations inside the broker
- complex routing DSLs

 The broker should remain focused on transport, persistence, delivery, and scalability.

 ## Architecture

 ### High-Level Architecture

```
                         ┌──────────────────────┐
                         │      Producers       │
                         │ Java / Go / Python   │
                         │ Rust / Other         │
                         └──────────┬───────────┘
                                    │
                              Binary Protocol
                                    │
                         ┌──────────▼───────────┐
                         │   Broker Gateway     │
                         │                      │
                         │ Connections          │
                         │ Authentication       │
                         │ Protocol             │
                         │ Routing              │
                         │ Backpressure         │
                         └──────────┬───────────┘
                                    │
             ┌──────────────────────┼──────────────────────┐
             │                      │                      │
             ▼                      ▼                      ▼
      ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
      │ Partition 0 │       │ Partition 1 │       │ Partition N │
      └──────┬──────┘       └──────┬──────┘       └──────┬──────┘
             │                     │                     │
             ▼                     ▼                     ▼
        Append Log            Append Log            Append Log
             │                     │                     │
             ▼                     ▼                     ▼
           NVMe                  NVMe                  NVMe
             │                     │                     │
             └─────────────────────┼─────────────────────┘
                                   │
                                   ▼
                            Consumer Groups
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
                  Java            Go           Python
```

 ## Core Design Principles

 ### 1\. Partition is the unit of parallelism

 There should be no global message-processing lock.

 Instead:

```
Topic
 ├── Partition 0
 ├── Partition 1
 ├── Partition 2
 ├── ...
 └── Partition N
```

 Each partition can be processed independently.

 ### 2\. Append-only storage

 Messages are written sequentially:

```
offset 0
offset 1
offset 2
offset 3
...
```

 This simplifies:

 - persistence
- recovery
- replication
- sequential I/O
- offset management

 ### 3\. Batch first

 The system is designed around batches:

```
Producer
   │
   ├── Message
   ├── Message
   ├── Message
   └── Message
          │
          ▼
        Batch
          │
          ▼
        Broker
```

 Batching reduces:

 - network syscalls
- protocol overhead
- storage operations
- synchronization overhead

 ### 4\. Backpressure is mandatory

 A producer must not be allowed to consume unlimited memory when consumers cannot keep up.

 The broker will enforce configurable limits for:

 - connection buffers
- partition buffers
- producer batches
- consumer fetches
- disk usage

 ### 5\. Explicit delivery semantics

 Delivery semantics must be documented rather than implied.

 Supported/planned semantics include:

 - at-most-once
- at-least-once
- configurable acknowledgement modes

 Exactly-once processing is not a first-release goal.

 ## Features

 ### Current

 Early development:

 - Rust workspace
- TCP server
- Binary frame protocol
- Basic producer operation
- Basic fetch operation
- Append-only storage prototype
- Message offsets
- Segmented WAL
- Sparse offset index
- Multiple partitions
- Consumer groups
- Retry mechanism
- Dead-letter queues
- Replication
- Cluster management
- Authentication
- TLS
- Metrics
- Java SDK
- Go SDK
- Python SDK
- Rust SDK
- Spring Boot starter

 ## Message Model

 A message conceptually contains:

```
Message
├── offset
├── timestamp
├── key
├── headers
└── payload
```

 Example:

```
{
  "key": "customer-123",
  "payload": "...",
  "headers": {
    "content-type": "application/json"
  }
}
```

 The production wire protocol will use a binary representation rather than JSON.

 ## Topics and Partitions

 A topic represents a logical stream:

```
orders
```

 A topic contains partitions:

```
orders
├── partition-0
├── partition-1
├── partition-2
└── partition-3
```

 Messages with the same key should normally map to the same partition:

```
partition = hash(key) % partition_count
```

 This allows per-key ordering while maintaining horizontal scalability.

 Global ordering across an entire distributed topic is intentionally not guaranteed.

 ## Producer Model

 Producers maintain persistent connections to brokers.

 Conceptually:

```
Producer producer =
    client.producer("orders");

producer.send(order);
```

 High-throughput producers should use batching:

```
Producer
   │
   ├── Message
   ├── Message
   ├── Message
   ├── ...
   └── Message
          │
          ▼
       Batch
          │
          ▼
        Broker
```

 The protocol supports request IDs so responses can be correlated with requests.

 ## Consumer Model

 Consumers fetch messages from partitions.

 Conceptually:

```
Consumer
    │
    ▼
Partition 3
    │
    ▼
offset 10000
    │
    ▼
batch
```

 After processing:

```
ACK offset 10999
```

 Acknowledging an offset can represent successful processing through that point.

 ## Consumer Groups

 Consumers can form groups:

```
orders
├── P0 ── Consumer A
├── P1 ── Consumer B
├── P2 ── Consumer C
├── P3 ── Consumer A
└── P4 ── Consumer B
```

 A partition should be actively assigned to only one consumer within a consumer group at a time.

 This enables horizontal scaling of consumers.

 ## Delivery Semantics

 ### At-most-once

 The message may be lost but should not be delivered more than once.

 Useful when:

 - latency is critical
- data loss is acceptable

 ### At-least-once

 The message should not be lost after successful persistence, but may be delivered more than once.

 Consumers should therefore implement idempotent processing where necessary.

 Example:

```
Message
   ↓
Process
   ↓
Crash
   ↓
No ACK
   ↓
Retry
   ↓
Process again
```

 ### Exactly-once

 Exactly-once processing is a future research/design area.

 The project will not initially claim exactly-once semantics simply because a message is stored once.

 ## Persistence

 The storage engine uses an append-only log.

 Conceptually:

```
partition/
├── segment-000000.log
├── segment-000001.log
└── segment-000002.log
```

 Each record contains metadata and payload.

 Future versions will use:

```
Segment
├── .log
└── .index
```

 The index will map offsets approximately to disk positions:

```
offset      position

0           0
1000        128 KB
2000        256 KB
3000        384 KB
```

 This allows efficient random fetches without maintaining a massive in-memory index.

 ## Replication

 The planned replication model is leader/follower based.

```
              Partition 7

             ┌──────────────┐
             │    Leader    │
             │   Broker A   │
             └──────┬───────┘
                    │
              ┌─────┴─────┐
              ▼           ▼
        ┌───────────┐ ┌───────────┐
        │ Broker B  │ │ Broker C  │
        │ Replica   │ │ Replica   │
        └───────────┘ └───────────┘
```

 Planned replication modes:

 - replication-factor = 1
- replication-factor = 2
- replication-factor = 3

 The cluster will eventually support quorum-based durability.

 ## Backpressure

 The broker must protect itself from overloaded producers and slow consumers.

 Example:

```
Producer
   │
   ▼
Bounded Buffer
   │
   ▼
Partition
   │
   ▼
Persistent Log
   │
   ▼
Consumer
```

 If capacity is exhausted, the broker may:

 - throttle
- reject
- delay
- return a retryable error

 depending on configuration.

 ## Protocol

 The initial protocol uses a binary frame:

```
┌────────────────┬─────────┬─────────┬──────────────┐
│ Length u32     │ Version │ Command │ Request ID   │
│                │ u8      │ u8      │ u64          │
├────────────────┴─────────┴─────────┴──────────────┤
│ Payload                                             │
└────────────────────────────────────────────────────┘
```

 Initial commands:

```
PRODUCE
PRODUCE_ACK

FETCH
FETCH_RESPONSE

ACK

ERROR
```

 Future commands:

```
SUBSCRIBE
HEARTBEAT
CREATE_TOPIC
DELETE_TOPIC
METADATA
JOIN_GROUP
LEAVE_GROUP
COMMIT_OFFSET
```

 The wire protocol will be versioned from the beginning.

 Backward compatibility will be treated as a first-class design concern.

 ## Client SDKs

 Planned official clients:

 | Language | Package | Status |
| --- | --- | --- |
| Rust | mq-client-rust | Planned |
| Java | mq-client-java | Planned |
| Go | mq-client-go | Planned |
| Python | mq-client-python | Planned |

The clients should expose consistent concepts:

```
Client
 ├── Producer
 ├── Consumer
 ├── Admin
 └── ConsumerGroup
```

 The implementation may differ by language, but the semantics should remain consistent.

 ## Spring Boot Integration

 A dedicated Spring integration is planned:

```
mq-client-java
       │
       ▼
spring-mq
       │
       ▼
spring-boot-starter-mq
```

 Example target API:

```
@MqListener(
    topic = "orders",
    group = "payment-service"
)
public void process(Order order) {
    paymentService.process(order);
}
```

 Target configuration:

```
mq:
  bootstrap-servers:
    - mq-01:9090
    - mq-02:9090
    - mq-03:9090

  producer:
    batch-size: 1000

  consumer:
    group: payment-service

  security:
    enabled: true
```

 Spring integration will remain an adapter layer and will not be coupled to the broker core.

 ## Security

 Security will be designed into the protocol and broker architecture rather than added as an afterthought.

 Planned capabilities:

 - TLS
- mutual TLS
- authentication
- authorization
- topic-level permissions
- consumer-group permissions
- administrative permissions
- credential rotation
- audit logging

 Potential authentication mechanisms:

 - TLS certificates
- API credentials
- JWT
- OIDC integration

 Security implementations will be evaluated independently before production adoption.

 ## Observability

 The broker will expose metrics suitable for Prometheus/OpenTelemetry-based environments.

 Planned metrics include:

 - messages produced
- messages consumed
- bytes produced
- bytes consumed
- producer latency
- consumer latency
- p50 latency
- p95 latency
- p99 latency
- p999 latency
- consumer lag
- active connections
- active producers
- active consumers
- partition size
- partition throughput
- disk read bytes
- disk write bytes
- replication lag
- failed requests
- retry count

 The system should also support structured logs and distributed tracing.

 ## Configuration

 Example configuration:

```
broker:
  node-id: 1

network:
  host: 0.0.0.0
  port: 9090

storage:
  data-dir: /var/lib/my-mq
  segment-size: 1GB

partitions:
  default-count: 128

producer:
  max-batch-size: 1000
  max-batch-bytes: 4MB

consumer:
  max-fetch-messages: 1000
  max-fetch-bytes: 4MB

durability:
  mode: batch

replication:
  factor: 3
```

 Configuration names are provisional during early development.

 ## Repository Structure

```
my-mq/
│
├── crates/
│   │
│   ├── mq-server/
│   │   └── src/
│   │
│   ├── mq-protocol/
│   │   └── src/
│   │
│   ├── mq-core/
│   │   └── src/
│   │
│   ├── mq-storage/
│   │   └── src/
│   │
│   └── mq-consumer/
│       └── src/
│
├── clients/
│   ├── java/
│   ├── go/
│   ├── python/
│   └── rust/
│
├── integrations/
│   └── spring-boot/
│
├── benchmarks/
│
├── docs/
│   ├── architecture/
│   ├── protocol/
│   ├── operations/
│   └── adr/
│
├── deploy/
│   ├── docker/
│   ├── kubernetes/
│   └── systemd/
│
├── examples/
│
├── Cargo.toml
└── README.md
```

 ## Technology Stack

 ### Broker

 - Rust
- Tokio
- bytes
- asynchronous TCP
- append-only storage
- NVMe-oriented storage architecture

 ### Clients

 - Java
- Go
- Python
- Rust

 ### Spring

 - Spring Boot
- Spring dependency injection
- Spring lifecycle management

 ### Observability

 Planned:

 - Prometheus
- OpenTelemetry
- structured logging

 ### Deployment

 Planned:

 - Linux
- Docker
- Kubernetes
- bare-metal servers

 ## Performance Strategy

 The system is designed around several independent scaling dimensions.

```
Throughput
    │
    ├── Batching
    │
    ├── Partition parallelism
    │
    ├── Broker parallelism
    │
    ├── Efficient networking
    │
    ├── Sequential storage
    │
    ├── Zero/minimal-copy data paths
    │
    └── Backpressure
```

 A conceptual scaling model:

```
                    Cluster
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
     Broker 1       Broker 2       Broker 3
        │              │              │
     P0..P31        P32..P63       P64..P95
        │              │              │
       NVMe           NVMe           NVMe
```

 The project does not assume that increasing the number of CPU cores automatically increases throughput.

 Every performance claim must be validated with reproducible benchmarks.

 ## Benchmarking

 Benchmark results must include:

 - Hardware
- OS
- CPU
- RAM
- Storage
- Network
- Broker configuration
- Partition count
- Replication factor
- Message size
- Batch size
- Producer count
- Consumer count
- Durability mode
- Compression

 Example benchmark definition:

```
CPU:               32 cores
RAM:               128 GB
Storage:           NVMe
Network:           25 GbE

Message size:      256 bytes
Partitions:        128
Producers:         100
Consumers:         100
Batch size:        1000
Replication:       3

Measure:
  throughput
  p50
  p95
  p99
  p999
  CPU
  RAM
  network
  disk
```

 Benchmark numbers without this context should not be treated as meaningful.

 ## Reliability

 The broker should tolerate:

 - client disconnects
- producer crashes
- consumer crashes
- broker process crashes
- machine restart
- incomplete writes
- corrupted records
- replica failure

 Recovery flow:

```
Broker starts
     │
     ▼
Open log
     │
     ▼
Validate records
     │
     ▼
Recover last valid offset
     │
     ▼
Rebuild index if necessary
     │
     ▼
Start serving
```

 ## Failure Handling

 ### Producer failure

 Unacknowledged messages may be retried by the client.

 Idempotent producer support is planned.

 ### Consumer failure

 Unacknowledged messages become available again according to consumer-group and acknowledgement semantics.

 ### Broker failure

 A replicated partition should eventually be promoted to another replica.

 ### Disk failure

 Replication should allow the cluster to continue serving data when sufficient replicas remain available.

 ## Deployment

 ### Single Node

 Development:

```
Application
     │
     ▼
MQ Broker
     │
     ▼
Local NVMe
```

 ### Production Cluster

```
                Load Balancer
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Broker 1   Broker 2   Broker 3
          │          │          │
          └──────┬───┴───┬──────┘
                 │       │
              Replication
```

 Kubernetes deployment will be considered after the broker's cluster semantics stabilize.

 The system should not depend on Kubernetes for correctness.

 ## Development Setup

 ### Requirements

 Recommended development environment:

 - Linux / macOS
- Rust stable
- Cargo
- Git

 For performance testing:

 - Linux
- NVMe SSD
- multi-core CPU
- 10/25/40\+ GbE where applicable

 Install Rust through the official Rust toolchain installer.

 Clone:

```
git clone <repository-url>
cd my-mq
```

 Build:

```
cargo build --workspace
```

 Run tests:

```
cargo test --workspace
```

 Run formatting:

```
cargo fmt --all
```

 Run linting:

```
cargo clippy --workspace --all-targets --all-features
```

 ## Running the Broker

 Start the development broker:

```
cargo run -p mq-server
```

 Default endpoint:

```
127.0.0.1:9090
```

 Data directory:

```
./data/
```

 Example:

```
data/
└── partition-0.log
```

 ## Example Producer

 Conceptually:

```
let client =
    Client::connect("mq://127.0.0.1:9090").await?;

let producer =
    client.producer("orders");

producer
    .send(order)
    .await?;
```

 The production client API is still under development.

 ## Example Consumer

 Target API:

```
let client =
    Client::connect("mq://127.0.0.1:9090").await?;

let consumer =
    client.consumer(
        "orders",
        "payment-service"
    );

while let Some(message) =
    consumer.next().await?
{
    process(message).await?;

    consumer.ack(message.offset()).await?;
}
```

 ## Roadmap

 ### Phase 0 — Foundation

 - Rust workspace
- TCP server
- Binary protocol
- Basic produce
- Basic fetch
- Basic WAL

 ### Phase 1 — Storage Engine

 - Segmented log
- Sparse offset index
- Log recovery
- CRC validation
- Retention
- Log compaction research
- Configurable flush policy

 ### Phase 2 — High Throughput

 - Partition workers
- Batch processing
- Bounded queues
- Backpressure
- Connection pooling
- Zero-copy optimizations
- Storage benchmarks
- Network benchmarks

 ### Phase 3 — Distributed Messaging

 - Multiple partitions
- Topic metadata
- Consumer groups
- Offset management
- Rebalancing
- Retry queues
- Dead-letter queues

 ### Phase 4 — Distributed Cluster

 - Broker discovery
- Partition ownership
- Leader election
- Replica management
- Replication
- Quorum
- Failure recovery
- Cluster metadata

 ### Phase 5 — Security

 - TLS
- Authentication
- Authorization
- ACL
- Audit logging

 ### Phase 6 — Client SDKs

 - Rust
- Java
- Go
- Python
- C# / .NET
- Node.js

 ### Phase 7 — Spring

 - Spring client
- Spring Boot starter
- @MqListener
- producer abstraction
- retry integration
- configuration properties
- health indicators
- Micrometer integration

 ### Phase 8 — Operations

 - Admin API
- Metrics
- OpenTelemetry
- Web UI
- Docker image
- Kubernetes deployment
- Helm chart
- Upgrade tooling

 ## Versioning

 The project follows semantic versioning once the public APIs stabilize:

```
MAJOR.MINOR.PATCH
```

 During early development:

```
0.x
```

 indicates that:

 - APIs may change
- protocol formats may change
- storage formats may change
- cluster semantics may change

 A stable protocol version will be established before the first production release.

 ## Compatibility

 Compatibility exists at several levels:

```
Client API
     │
     ▼
Wire Protocol
     │
     ▼
Broker
     │
     ▼
Storage
```

 Protocol compatibility should be preserved independently from internal Rust implementation changes.

 Storage format changes must include an explicit migration or versioning strategy before production release.

 ## Testing Strategy

 The project will use multiple levels of testing.

 ### Unit Tests

 Test:

 - protocol encoding
- protocol decoding
- offset handling
- partition logic
- storage records
- index logic

 ### Integration Tests

 Test:

 - producer → broker
- broker → consumer
- restart recovery
- consumer groups
- retry behavior

 ### Failure Tests

 Test:

 - process crash
- network disconnect
- disk-full
- partial write
- corrupted record
- broker failure
- replica failure
- consumer failure
- producer failure

 ### Stress Tests

 Test:

```
1K
10K
100K
1M
5M
10M
```

 messages/sec where hardware permits.

 ## Performance Acceptance Criteria

 Performance targets will be defined by workload rather than a single marketing number.

 Example acceptance test:

```
Message size:       256 bytes
Batch size:         1000
Partitions:         128
Replication:        1

Target:
    >= 1M msg/sec

And:
    p99 latency < defined threshold

While:
    no data loss
    no memory leak
    no unbounded queue growth
```

 For replicated production workloads:

```
Replication factor: 3
```

 will be benchmarked separately.

 A benchmark result will only be considered valid if it includes complete hardware and configuration information.

 ## Architecture Decision Records

 Important architectural decisions should be documented under:

```
docs/adr/
```

 Examples:

```
ADR-0001-use-rust-for-broker.md
ADR-0002-partition-as-unit-of-parallelism.md
ADR-0003-binary-wire-protocol.md
ADR-0004-append-only-storage.md
ADR-0005-consumer-group-model.md
ADR-0006-replication-strategy.md
ADR-0007-delivery-semantics.md
```

 Architecture decisions should document:

 - Context
- Problem
- Options
- Decision
- Consequences
- Alternatives rejected

 This prevents architectural knowledge from existing only in source code or individual developers' heads.

 ## Contribution Guidelines

 Contributions are welcome.

 Before submitting a pull request:

```
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

 Performance-sensitive changes should include benchmarks.

 Protocol changes should include:

 - compatibility considerations
- versioning considerations
- tests
- documentation

 Storage changes should include:

 - crash-recovery considerations
- migration considerations
- corruption handling
- benchmark results

 ## Security Policy

 Please do not publicly disclose security vulnerabilities before the project has an appropriate security response process.

 A dedicated security policy and reporting mechanism will be established before production release.

 Security-sensitive changes should receive additional review.

 ## License

 This project is intended to be released under the Apache License 2.0.

 See LICENSE for details.
