# Async Order Book in Python — Development Design Document

## 1. Project Purpose

Build an asynchronous order book system in Python as a learning project focused on:

- asynchronous programming
- event-driven architecture
- queues and backpressure
- task lifecycle and cancellation
- network programming
- market-data processing
- deterministic state management
- testing concurrent systems
- exchange and order-book mechanics

This project is not intended to be a production-grade exchange, a high-frequency trading system, or a live order-routing system. Its purpose is to develop a strong mental model of how asynchronous services coordinate work while preserving correctness.

The central architectural rule is:

> Keep order-book state transitions synchronous and deterministic. Use asynchronous components to receive, route, publish, persist, and replay messages around the book.

---

## 2. Learning Objectives

By the end of the project, you should be able to explain:

1. The difference between concurrency and parallelism.
2. How the Python event loop schedules coroutines.
3. What causes a coroutine to yield control.
4. Why race conditions can still occur in a single-threaded async program.
5. How queues decouple producers from consumers.
6. What backpressure is and why bounded queues matter.
7. Why a single-writer architecture simplifies shared-state correctness.
8. How price-time priority works in a central limit order book.
9. How market-data snapshots and incremental updates rebuild a book.
10. How sequence numbers support ordering, recovery, and replay.
11. How graceful shutdown and task cancellation should be handled.
12. How to test deterministic logic separately from asynchronous orchestration.
13. How to detect stale, duplicated, missing, or out-of-order market-data messages.
14. Why async improves I/O concurrency but does not automatically make CPU-bound work faster.

---

## 3. Scope

### Version 1: Core Matching Engine

Support:

- one process
- one event loop
- one or more symbols
- limit orders
- market orders
- buy and sell sides
- price-time priority
- partial fills
- full fills
- cancellations
- order modifications
- top-of-book queries
- aggregated depth queries
- trade events
- deterministic command processing
- append-only event logging
- deterministic replay

### Version 2: Async Service

Add:

- multiple simulated clients
- bounded command queues
- event publishing
- multiple event subscribers
- slow-consumer handling
- graceful shutdown
- metrics
- TCP or WebSocket connectivity

### Version 3: External Market-Data Consumer

Add:

- connection to a public market-data WebSocket
- snapshot processing
- incremental update processing
- sequence validation
- heartbeat monitoring
- reconnection
- resynchronization after gaps
- local order-book reconstruction

### Explicitly Out of Scope Initially

Do not add these until the core project is complete:

- real-money order submission
- exchange authentication
- database clusters
- Redis
- Kafka
- multiprocessing
- multithreading
- graphical interfaces
- cloud deployment
- custom binary protocols
- Cython
- lock-free structures
- distributed matching
- high-frequency performance claims
- production security guarantees

---

## 4. Core Architectural Decision

### 4.1 Single-Writer Ownership

Each order book should have one logical owner responsible for all mutations.

All external components submit commands to that owner. The owner processes one command completely before beginning the next command.

This creates a serialized state transition stream:

```text
Command 1 → Apply fully → Emit events
Command 2 → Apply fully → Emit events
Command 3 → Apply fully → Emit events
```

This design avoids several common problems:

- two tasks modifying the same price level simultaneously
- one task reading an order while another cancels it
- partial state updates being observed by another task
- unclear lock ownership
- nondeterministic replay

### 4.2 Async Around the Book, Not Inside Every Operation

The order book itself should behave like a deterministic state machine.

It receives a command, changes state, and produces zero or more events.

Async belongs around this core:

```text
Clients / Feeds
      │
      ▼
Command Queue
      │
      ▼
Matching Engine
      │
      ├── Trade Events
      ├── Order Events
      ├── Book Events
      └── Audit Events
              │
              ▼
          Event Bus
      ┌───────┼────────┐
      ▼       ▼        ▼
   Logger   Metrics   Network Publishers
```

### 4.3 Why Not Make Every Book Method Async?

A method should be asynchronous when it must wait for something external, such as:

- network input
- network output
- a queue
- a timer
- a file operation delegated to an async-compatible layer
- another coroutine

Order matching is primarily in-memory state mutation. Marking it async would not make it faster and would introduce opportunities for accidental interleaving.

---

## 5. Domain Model

Define the conceptual entities before implementation.

### 5.1 Order

An order should contain:

- unique order ID
- client ID
- symbol
- side
- order type
- original quantity
- remaining quantity
- limit price, when applicable
- creation timestamp
- sequence number assigned by the engine
- current status

Suggested statuses:

- pending
- accepted
- partially filled
- filled
- cancelled
- rejected

### 5.2 Trade

A trade should contain:

- trade ID
- symbol
- execution price
- execution quantity
- maker order ID
- taker order ID
- maker client ID
- taker client ID
- engine sequence number
- execution timestamp

### 5.3 Price Level

A price level represents:

- one price
- ordered queue of resting orders
- total remaining quantity
- number of active orders

Orders at the same price must preserve FIFO priority.

### 5.4 Order Book

Each order book contains:

- bid side
- ask side
- active-order lookup
- symbol
- current sequence number
- cumulative traded volume
- optional book version

### 5.5 Command

Commands represent requested state changes.

Initial command types:

- submit order
- cancel order
- modify order
- request snapshot
- shutdown engine

### 5.6 Event

Events describe facts that have already occurred.

Initial event types:

- order accepted
- order rejected
- order partially filled
- order filled
- order cancelled
- cancel rejected
- order modified
- trade executed
- top of book changed
- depth changed
- engine started
- engine stopped
- sequence gap detected
- subscriber disconnected

Commands are requests. Events are facts.

---

## 6. Matching Rules

Document the matching policy before implementation.

### 6.1 Price Priority

For bids:

- higher prices have priority over lower prices

For asks:

- lower prices have priority over higher prices

### 6.2 Time Priority

At the same price:

- the earliest accepted resting order executes first

### 6.3 Crossing Conditions

A buy limit order can execute when:

```text
buy limit price >= best ask
```

A sell limit order can execute when:

```text
sell limit price <= best bid
```

### 6.4 Execution Price

Use the resting order's price as the trade price.

This means the incoming order is the taker and the resting order is the maker.

### 6.5 Partial Fills

An incoming order may:

- fill one resting order partially
- fully fill one resting order and continue
- sweep several price levels
- remain partially unfilled after all eligible liquidity is exhausted

### 6.6 Market Orders

A market order:

- trades against the best available prices
- continues until filled or the opposite side is empty
- never rests on the book
- is cancelled or expired for any unfilled remainder

### 6.7 Modification Policy

Use a simple, documented rule:

- reducing quantity retains time priority
- increasing quantity loses time priority
- changing price loses time priority
- changing side is not permitted
- changing symbol is not permitted

A priority-losing modification should be treated conceptually as cancellation followed by a new submission.

### 6.8 Cancellation Policy

A cancellation succeeds only when:

- the order exists
- the order is still active
- the requesting client owns the order, if ownership checks are included

---

## 7. Data Structure Strategy

The goal is learning and correctness before optimization.

### 7.1 Initial Representation

Conceptually use:

- a mapping from price to FIFO order queue for bids
- a mapping from price to FIFO order queue for asks
- a mapping from order ID to active order metadata

This makes price levels and time priority easy to inspect.

### 7.2 Best-Price Lookup

Begin with the simplest understandable approach.

Later, compare alternatives:

- scanning current prices
- maintaining sorted price collections
- using heaps with lazy deletion
- using third-party sorted containers
- maintaining explicit best-price pointers

Do not optimize until benchmarks demonstrate a bottleneck.

### 7.3 Price Representation

Do not use binary floating-point values as price keys.

Use one of:

- integer ticks
- integer cents
- fixed decimal values

Integer ticks are the simplest approach for this project.

### 7.4 Quantity Representation

Use whole-number quantities unless you intentionally model fractional assets.

If fractional quantities are added later, use a fixed precision rather than floating-point values.

---

## 8. System Components

### 8.1 Command Producers

Command producers may include:

- simulated retail client
- simulated market maker
- random-order generator
- cancellation generator
- network client handler
- replay reader
- administrative control process

Each producer submits commands to a bounded command queue.

### 8.2 Command Queue

Responsibilities:

- decouple producers from the engine
- preserve arrival order
- provide backpressure
- expose queue depth for monitoring
- support controlled shutdown

Questions to answer:

- What is the maximum queue size?
- What happens when the queue is full?
- Are commands rejected, delayed, or timed out?
- Can shutdown commands bypass normal queue traffic?
- Does every accepted command receive an acknowledgment?

### 8.3 Matching Engine

Responsibilities:

- assign engine sequence numbers
- validate commands
- route commands by symbol
- apply commands to the appropriate book
- emit events
- preserve deterministic ordering
- expose health state
- stop cleanly

The matching engine should not:

- write directly to slow network clients
- perform blocking file writes
- call external APIs
- wait on database queries
- run expensive analytics
- contain exchange-specific WebSocket parsing

### 8.4 Event Bus

Responsibilities:

- receive events from the engine
- deliver events to subscribers
- isolate slow consumers
- track subscriber lifecycle
- define delivery behavior

Potential subscribers:

- audit logger
- trade logger
- metrics collector
- WebSocket publisher
- terminal display
- snapshot writer
- alerting process

### 8.5 Persistence Writer

Responsibilities:

- write commands or events to an append-only log
- preserve sequence order
- flush safely
- expose write failures
- support replay

Do not let disk latency block the matching engine indefinitely.

### 8.6 Snapshot Service

Responsibilities:

- periodically request a consistent book snapshot
- record the sequence number associated with the snapshot
- serialize active orders and price levels
- support faster recovery than replaying from the beginning

### 8.7 Network Gateway

Responsibilities:

- accept client connections
- decode incoming messages
- validate message structure
- normalize messages into internal commands
- submit commands
- return acknowledgments
- publish subscribed events
- disconnect malformed or persistently slow clients

The network gateway should not modify book state directly.

### 8.8 Market-Data Adapter

Responsibilities:

- connect to an external feed
- parse venue-specific messages
- normalize them into internal update types
- monitor sequence continuity
- handle heartbeats
- reconnect after failure
- trigger a full resynchronization when required

Keep venue-specific formats outside the core book.

---

## 9. Async Concepts to Learn Through the Project

### 9.1 Event Loop

Understand:

- how the event loop schedules ready tasks
- why a coroutine runs until it reaches a suspension point
- why long CPU-bound work blocks all other tasks
- how timers and I/O readiness return tasks to the runnable set

### 9.2 Coroutines

Understand:

- coroutine creation versus execution
- awaiting a coroutine
- task creation
- coroutine return values
- exception propagation

### 9.3 Tasks

Understand:

- when to create a task
- how task exceptions are surfaced
- why unmanaged background tasks are dangerous
- structured concurrency
- task groups
- shutdown coordination

### 9.4 Queues

Understand:

- producer-consumer design
- bounded versus unbounded queues
- queue depth
- fairness
- acknowledgment of completed work
- timeouts
- queue shutdown policy

### 9.5 Backpressure

Backpressure occurs when producers create work faster than consumers can process it.

Explore these policies:

- block the producer
- reject new work
- drop old work
- drop new work
- coalesce updates
- disconnect slow clients
- scale consumers, where safe

Different message types may need different policies.

Orders should generally not be silently dropped.

Replaceable market-data snapshots may sometimes be coalesced.

### 9.6 Cancellation

Understand:

- cancellation as a normal control-flow event
- cleanup in finally blocks
- cancellation propagation
- shielding critical cleanup
- graceful versus forced shutdown
- timeout handling

### 9.7 Race Conditions in Async Code

A single event-loop thread does not eliminate race conditions.

A race can occur when:

1. a task reads shared state
2. the task awaits
3. another task changes that state
4. the first task resumes using stale assumptions

The single-writer engine minimizes this risk by preventing multiple tasks from mutating the same book.

### 9.8 Locks and Synchronization Primitives

Learn what these tools do, but avoid using them by default:

- lock
- event
- condition
- semaphore
- barrier

Use ownership and message passing before shared mutable state plus locks.

---

## 10. Event Ordering and Sequence Numbers

Every accepted engine command should receive a monotonically increasing sequence number.

Sequence numbers support:

- deterministic replay
- auditability
- missing-message detection
- duplicate detection
- ordering across subscribers
- snapshot recovery
- debugging

### Ordering Questions

Document answers to:

- Is the sequence assigned when a command arrives or when processing begins?
- Can rejected commands consume sequence numbers?
- Do all events from one command share a sequence number?
- Do individual events also receive sub-sequence values?
- Is ordering global or per symbol?
- Can subscribers observe events out of order?

For the first version, use one global engine sequence.

---

## 11. Event Delivery Semantics

Choose and document delivery guarantees.

Possible semantics:

### At-Most-Once

An event may be lost but is never delivered more than once.

### At-Least-Once

An event may be duplicated but should eventually be delivered.

### Exactly-Once

Very difficult across system boundaries and usually requires transactional coordination.

For this learning project:

- preserve exactly-once processing inside the in-memory matching engine
- treat external publication as at-most-once initially
- add idempotent sequence handling for replay and reconnect scenarios
- do not claim distributed exactly-once delivery

---

## 12. Slow Consumer Policy

A slow logger, network subscriber, or metrics process must not freeze the entire system indefinitely.

Possible responses:

1. Block the publisher.
2. Buffer up to a fixed limit.
3. Drop replaceable updates.
4. Disconnect the subscriber.
5. Send a fresh snapshot and resume.
6. Record subscriber lag and alert.

Recommended initial policy:

- audit log: block briefly, then fail the system visibly if durability is required
- metrics: drop or aggregate updates when overloaded
- UI market-data subscriber: disconnect or send the latest snapshot
- trade subscriber: bounded buffer, then disconnect with an explicit error

---

## 13. Persistence and Replay

### 13.1 Append-Only Log

Record enough information to reconstruct state.

Options:

- command log
- event log
- both

A command log is useful for replaying engine decisions.

An event log is useful for auditing what the engine produced.

For the first version, record both in separate logical streams or clearly distinguish record types.

### 13.2 Replay Requirements

A replay should reconstruct:

- final active orders
- all trades
- total volume
- best bid and ask
- aggregated depth
- final sequence number

The replayed state should exactly match the original state.

### 13.3 Snapshot Recovery

Recovery flow:

```text
Load latest snapshot
        │
        ▼
Read snapshot sequence number
        │
        ▼
Replay later log records
        │
        ▼
Validate final state
```

### 13.4 Determinism

Avoid nondeterministic inputs inside matching logic.

External timestamps, random values, and generated IDs should be assigned before or at the engine boundary and recorded in the log.

---

## 14. External Market-Data Book

A market-data reconstruction book is distinct from your matching engine.

### Matching Engine

- accepts orders
- decides executions
- generates trades
- owns the source of truth

### Market-Data Reconstruction

- receives updates from another venue
- applies externally determined changes
- validates message ordering
- attempts to mirror the venue's book

### Required Feed Behaviors

Study and implement:

- subscription
- initial snapshot
- incremental updates
- heartbeat
- sequence numbers
- duplicate updates
- stale messages
- out-of-order messages
- missing messages
- reconnect
- full resynchronization

### Recovery Rule

When a sequence gap is detected:

1. mark the local book invalid
2. stop publishing it as current
3. reconnect or request a new snapshot
4. rebuild state
5. resume only after continuity is restored

Never silently continue after an unexplained sequence gap.

---

## 15. Testing Strategy

Separate deterministic order-book tests from async orchestration tests.

### 15.1 Unit Tests: Matching Logic

Test:

- resting buy order
- resting sell order
- buy crossing best ask
- sell crossing best bid
- partial fill
- full fill
- multiple fills at one price
- sweep through multiple prices
- FIFO priority
- better-price priority
- cancellation
- cancellation of missing order
- duplicate order ID
- invalid quantity
- invalid price
- market order with no liquidity
- residual market-order quantity
- modification that retains priority
- modification that loses priority
- empty price-level cleanup

### 15.2 Invariant Tests

After every command, verify:

- active order IDs are unique
- every active order appears exactly once
- every queued order exists in the lookup
- every lookup order exists in a price level
- remaining quantity is positive
- empty price levels do not remain
- resting bids do not cross resting asks
- price-level quantity equals the sum of its orders
- executed volume is conserved
- FIFO order remains valid
- filled and cancelled orders are absent from the active lookup

### 15.3 Property-Based Tests

Generate random valid command sequences and check invariants after every step.

Useful properties:

- replay produces the same final state
- total submitted quantity equals active quantity plus filled quantity plus cancelled remainder
- processing identical command logs produces identical event logs
- no trade quantity is negative or zero
- no order is filled beyond its original quantity

### 15.4 Async Integration Tests

Test:

- several producers submitting concurrently
- commands processed in engine order
- bounded queue behavior
- producer timeout
- slow subscriber
- subscriber disconnection
- task failure propagation
- graceful shutdown
- forced cancellation
- queued work during shutdown
- event ordering
- replay writer failure
- network client disconnect
- heartbeat timeout

### 15.5 Failure Injection

Intentionally simulate:

- dropped WebSocket connection
- malformed message
- invalid sequence number
- duplicate message
- out-of-order update
- disk writer failure
- full queue
- stalled subscriber
- task exception
- shutdown during heavy traffic

A concurrent system is not complete until failure behavior is understood.

---

## 16. Observability

Track at least:

### Engine Metrics

- commands received
- commands accepted
- commands rejected
- commands processed per second
- average processing latency
- maximum processing latency
- command queue depth
- current sequence number

### Book Metrics

- active orders
- bid levels
- ask levels
- spread
- best bid
- best ask
- traded volume
- cancellation count
- fill count

### Subscriber Metrics

- subscriber count
- subscriber queue depth
- dropped updates
- disconnections
- publishing lag

### Feed Metrics

- connection status
- last message time
- last heartbeat time
- current sequence
- sequence-gap count
- reconnection count
- resynchronization count

Use monotonic time for elapsed-duration measurement.

---

## 17. Project Milestones

## Milestone 0 — Preparation

### Read

- Python async overview
- coroutine and task documentation
- queue documentation
- synchronization primitives
- basic order-book mechanics

### Write

Create a short note explaining:

- concurrency versus parallelism
- why the book is synchronous
- why the system around it is asynchronous
- how single-writer ownership prevents races

### Completion Criteria

You can explain the architecture without discussing implementation syntax.

---

## Milestone 1 — Order-Book Specification

### Design

Document:

- supported order types
- matching rules
- execution-price rule
- partial-fill behavior
- cancellation behavior
- modification behavior
- invalid-order rules
- market-order remainder behavior
- price and quantity representation

### Deliverable

A written matching specification with worked examples.

### Completion Criteria

Another developer could implement the matching behavior from your specification without guessing.

---

## Milestone 2 — Deterministic Core Book

### Build

Implement the in-memory book without networking or async orchestration.

### Focus

- correctness
- price-time priority
- active-order indexing
- event generation
- invariants

### Completion Criteria

- all unit tests pass
- invariant validation passes
- repeated identical inputs produce identical outputs
- no async code is required to test matching

---

## Milestone 3 — Async Fundamentals Exercises

Before integrating the order book, complete small isolated exercises covering:

- several concurrent timer tasks
- one producer and one consumer
- several producers and one consumer
- bounded queue saturation
- cancellation
- task groups
- graceful shutdown
- exception propagation

### Completion Criteria

You can predict which tasks may interleave and identify every suspension point.

---

## Milestone 4 — Async Matching Service

### Build

Add:

- command queue
- single engine consumer
- several simulated clients
- command acknowledgments
- event output
- shutdown command

### Completion Criteria

- simulated clients run concurrently
- book mutations remain serialized
- all accepted commands receive outcomes
- shutdown does not lose acknowledged work
- queue saturation behavior is documented

---

## Milestone 5 — Event Bus

### Build

Add:

- event subscribers
- separate subscriber queues
- logging subscriber
- metrics subscriber
- terminal display subscriber
- slow-consumer policy

### Completion Criteria

- every active subscriber receives appropriate events
- one slow subscriber does not silently freeze the system
- subscriber lag is measurable
- event ordering is validated

---

## Milestone 6 — Persistence and Replay

### Build

Add:

- append-only command log
- append-only event log
- sequence validation
- replay tool
- snapshot format

### Completion Criteria

- replay reconstructs identical final state
- replay reproduces identical trades
- snapshot plus later records matches full replay
- corrupted or missing sequence records are detected

---

## Milestone 7 — Network Server

### Choose One First

Recommended learning order:

1. raw TCP with newline-delimited JSON
2. WebSocket server
3. optional HTTP snapshot endpoint

### Build

Add:

- multiple simultaneous clients
- message validation
- command submission
- acknowledgments
- event subscriptions
- connection cleanup
- heartbeat or idle timeout

### Completion Criteria

- multiple clients can connect concurrently
- malformed messages do not crash the engine
- disconnected clients are cleaned up
- slow clients are handled according to policy
- network code never directly mutates the book

---

## Milestone 8 — External WebSocket Feed

### Build

Add a public market-data feed adapter.

Recommended initial venue:

- Coinbase Advanced Trade public market-data WebSocket

### Focus

- connection lifecycle
- channel subscriptions
- snapshot and update semantics
- heartbeats
- sequence validation
- reconnect
- stale-book detection

### Completion Criteria

- local book initializes correctly
- updates maintain continuity
- connection loss is detected
- sequence gaps invalidate the book
- recovery rebuilds a valid book
- venue-specific messages remain isolated in the adapter

---

## Milestone 9 — Benchmarking and Review

### Measure

- matching throughput
- latency distribution
- queue growth under load
- memory use
- subscriber lag
- snapshot cost
- replay speed

### Compare

- simple best-price lookup
- heap-based lookup
- different queue sizes
- one versus many symbols
- logging enabled versus disabled

### Completion Criteria

You can explain actual bottlenecks using measurements rather than assumptions.

---

## 18. Suggested Repository Documentation

Create these documents:

```text
README.md
docs/
  architecture.md
  matching-rules.md
  commands-and-events.md
  concurrency-model.md
  failure-handling.md
  persistence-and-replay.md
  external-feed.md
  testing-strategy.md
  benchmarks.md
```

### README Contents

Include:

- project purpose
- learning goals
- architecture diagram
- supported features
- current milestone
- how to run tests
- known limitations
- future work

### Architecture Decision Records

For major choices, record:

- context
- decision
- alternatives considered
- consequences

Suggested decisions to document:

1. Single-writer ownership
2. Integer tick prices
3. Bounded queues
4. Separate queues per subscriber
5. Append-only logging
6. Global sequence numbers
7. TCP before WebSockets
8. External feed adapter isolation

---

## 19. Recommended Reading Path

## Phase A — Python Async Foundations

### Official Python Documentation

1. **asyncio overview**  
   https://docs.python.org/3/library/asyncio.html

2. **Coroutines and tasks**  
   https://docs.python.org/3/library/asyncio-task.html

3. **Asyncio queues**  
   https://docs.python.org/3/library/asyncio-queue.html

4. **Synchronization primitives**  
   https://docs.python.org/3/library/asyncio-sync.html

5. **Developing with asyncio**  
   https://docs.python.org/3/library/asyncio-dev.html

6. **Asyncio streams**  
   https://docs.python.org/3/library/asyncio-stream.html

7. **High-level asyncio API index**  
   https://docs.python.org/3/library/asyncio-api-index.html

### Supplementary Explanation

8. **Real Python: Async IO in Python**  
   https://realpython.com/async-io-python/

Read the official overview first. Use Real Python to reinforce the mental model, then return to the official task and queue documentation.

---

## Phase B — Data Structures

1. **Python collections: deque**  
   https://docs.python.org/3/library/collections.html#collections.deque

2. **Python heap queue algorithm**  
   https://docs.python.org/3/library/heapq.html

3. **Python dataclasses**  
   https://docs.python.org/3/library/dataclasses.html

4. **Python decimal arithmetic**  
   https://docs.python.org/3/library/decimal.html

Questions to answer while reading:

- Why is a deque appropriate for FIFO orders at one price?
- Why does a heap not directly support efficient arbitrary deletion?
- What is lazy deletion?
- Why should prices avoid binary floating-point representation?

---

## Phase C — Market Microstructure and Order Books

### Free References

1. **Nasdaq TotalView-ITCH 5.0 Specification**  
   https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf

2. **Nasdaq ITCH information page**  
   https://www.nasdaqtrader.com/Trader.aspx?id=ITCH

3. **Coinbase Exchange matching-engine overview**  
   https://docs.cdp.coinbase.com/exchange/concepts/matching-engine

4. **Coinbase Exchange order types**  
   https://docs.cdp.coinbase.com/exchange/concepts/trading-and-orders

### Books

These are optional and may not be free:

- *Trading and Exchanges* — Larry Harris
- *Algorithmic and High-Frequency Trading* — Álvaro Cartea, Sebastian Jaimungal, José Penalva
- *Market Microstructure Theory* — Maureen O'Hara

For this project, read only enough microstructure material to understand:

- bid and ask
- spread
- liquidity
- maker and taker
- price priority
- time priority
- partial execution
- market impact
- order lifecycle

Do not let theoretical market microstructure delay the systems work.

---

## Phase D — Networking

### Python TCP Streams

1. **Asyncio streams**  
   https://docs.python.org/3/library/asyncio-stream.html

2. **Socket programming HOWTO**  
   https://docs.python.org/3/howto/sockets.html

### WebSockets Library

Use the current `websockets` asyncio implementation.

1. **websockets documentation home**  
   https://websockets.readthedocs.io/en/stable/

2. **Asyncio client reference**  
   https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html

3. **Asyncio server reference**  
   https://websockets.readthedocs.io/en/stable/reference/asyncio/server.html

4. **Tutorial: send and receive**  
   https://websockets.readthedocs.io/en/stable/intro/tutorial1.html

5. **Server FAQ**  
   https://websockets.readthedocs.io/en/stable/faq/server.html

6. **Client FAQ**  
   https://websockets.readthedocs.io/en/stable/faq/client.html

7. **Keepalive and latency**  
   https://websockets.readthedocs.io/en/stable/topics/keepalive.html

8. **Upgrade guide for the new asyncio implementation**  
   https://websockets.readthedocs.io/en/stable/howto/upgrade.html

Avoid old tutorials that use the legacy `websockets` API without checking the current documentation.

---

## Phase E — Coinbase Public Market Data

### Core Documentation

1. **Advanced Trade WebSocket overview**  
   https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview

2. **WebSocket channels**  
   https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels

3. **WebSocket setup guide**  
   https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/guides/websocket

4. **WebSocket rate limits**  
   https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-rate-limits

5. **WebSocket authentication**  
   https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-authentication

Authentication is not required for most public market-data channels, but read the authentication page so you understand the distinction between public market data and user-specific order data.

### Channels to Study

Focus on:

- heartbeats
- level2
- market trades
- status

Read channel-specific documentation from the WebSocket channel reference before implementation.

### Questions to Answer

- Does the channel provide a snapshot?
- How are updates represented?
- Is quantity absolute or incremental?
- Is there a sequence number?
- How are updates grouped?
- How often must heartbeats arrive?
- What causes subscriptions to close?
- How should the client recover from a gap?
- Are messages authenticated or public?
- What are the connection and message rate limits?

---

## Phase F — Testing

1. **pytest documentation**  
   https://docs.pytest.org/en/stable/

2. **pytest-asyncio documentation**  
   https://pytest-asyncio.readthedocs.io/en/stable/

3. **Hypothesis documentation**  
   https://hypothesis.readthedocs.io/en/latest/

4. **Hypothesis stateful testing**  
   https://hypothesis.readthedocs.io/en/latest/stateful.html

Stateful property-based testing is especially valuable for an order book because random sequences of submit, cancel, modify, and fill operations expose edge cases that example-based tests may miss.

---

## Phase G — Performance and Profiling

1. **Python timeit**  
   https://docs.python.org/3/library/timeit.html

2. **Python profiling documentation**  
   https://docs.python.org/3/library/profile.html

3. **Python tracemalloc**  
   https://docs.python.org/3/library/tracemalloc.html

4. **pyperf**  
   https://pyperf.readthedocs.io/en/latest/

Measure before optimizing.

Useful measurements:

- commands per second
- matching latency
- p50, p95, and p99 latency
- queue wait time
- event publishing time
- memory per active order
- snapshot serialization time
- replay speed

---

## 20. Questions to Answer in Your Own Notes

Before each milestone, write answers to these.

### Async Model

- What exactly makes one task yield to another?
- Which operations in my system can block the event loop?
- Where are my suspension points?
- Which task owns each mutable object?
- Can two tasks mutate the same state?
- What happens when a task fails?

### Queue Design

- Why is each queue bounded?
- What does queue saturation mean?
- Which messages may be dropped?
- Which messages must never be dropped silently?
- How is queue lag measured?
- How does shutdown interact with queued work?

### Matching Engine

- What determines maker and taker?
- What price is used for execution?
- How is time priority preserved?
- What happens to unfilled market-order quantity?
- What modifications lose priority?
- What invariants must always hold?

### Event System

- Does each subscriber receive every event?
- What happens when one subscriber is slow?
- Can events be duplicated?
- Can events be lost?
- How does a subscriber recover?
- Are events globally ordered?

### External Feed

- How is the initial state obtained?
- How are later updates applied?
- How is continuity verified?
- When is the local book considered stale?
- What triggers resynchronization?
- How is reconnection tested?

---

## 21. Final Project Definition of Done

The project is complete when:

- matching behavior is fully specified
- the synchronous core passes deterministic tests
- price-time priority is correct
- invariants hold under randomized command sequences
- multiple clients can submit commands concurrently
- one engine task owns all state mutation
- queues are bounded and overload behavior is documented
- events are published to multiple subscribers
- slow subscribers are handled intentionally
- shutdown is graceful and tested
- commands and events can be persisted
- replay reconstructs identical state
- a network gateway accepts multiple connections
- a public WebSocket feed can be consumed
- sequence gaps and stale data are detected
- reconnection and resynchronization work
- performance is measured
- limitations are honestly documented

---

## 22. Recommended Build Order

Follow this order strictly:

1. Write matching rules.
2. Define commands, events, and invariants.
3. Build the synchronous order book.
4. Test the synchronous order book thoroughly.
5. Complete isolated asyncio exercises.
6. Add the bounded command queue.
7. Add the single matching-engine task.
8. Add simulated concurrent clients.
9. Add event subscribers.
10. Add slow-consumer handling.
11. Add persistence.
12. Add deterministic replay.
13. Add raw TCP networking.
14. Add WebSocket networking.
15. Add the external Coinbase adapter.
16. Add recovery and resynchronization.
17. Benchmark only after correctness is established.

The biggest mistake would be connecting to a live WebSocket before the core book, event ordering, and recovery model are understood.