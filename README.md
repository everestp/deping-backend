<div align="center">

# ⬡ DePIN Uptime Monitor — Go Backend

**Decentralized, incentivized website uptime monitoring on Solana**

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat-square&logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![gRPC](https://img.shields.io/badge/gRPC-bidirectional-orange?style=flat-square&logo=grpc)](https://grpc.io)
[![Solana](https://img.shields.io/badge/Solana-Anchor-9945FF?style=flat-square)](https://anchor-lang.com)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-AMQP-FF6600?style=flat-square&logo=rabbitmq)](https://rabbitmq.com)

Website owners deposit SPL tokens to fund uptime monitoring. A global network of independent node runners (miners) execute HTTP checks from diverse geographic locations, earn token rewards for verified work, and settle on-chain automatically when their balance crosses a threshold.

[Quick Start](#-quick-start) · [Architecture](#-architecture) · [API Reference](#-api-reference) · [Configuration](#-configuration) · [Deployment](#-deployment) · [Security](#-security-model)

</div>

---

## Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [API Reference](#-api-reference)
- [gRPC / Proto](#-grpc--proto)
- [Database Schema](#-database-schema)
- [Background Workers](#-background-workers)
- [Anti-Cheat System](#-anti-cheat-system)
- [Reward Engine](#-reward-engine)
- [Solana Integration](#-solana-integration)
- [Security Model](#-security-model)
- [Deployment](#-deployment)
- [Observability](#-observability)
- [Development Guide](#-development-guide)
- [Production Checklist](#-production-checklist)
- [Known Limitations & TODOs](#-known-limitations--todos)

---

## 🔭 Overview

### What this system does

| Actor | Role |
|---|---|
| **Website Owner** | Deposits SPL tokens, registers URLs to monitor, views uptime dashboard |
| **Node Runner (Miner)** | Runs the Rust CLI binary, executes HTTP checks, earns token rewards |
| **Go Backend** | Coordinates everything — job dispatch, result validation, reward accounting, chain settlement |
| **Solana / Anchor** | Final source of truth for on-chain token balances and reward vault |

### Why DePIN

Centralized uptime monitors have a single point of failure and a single geographic perspective. This system replaces them with a trustless network of independent runners distributed worldwide. Consensus across multiple independent nodes from different regions eliminates both false positives and the ability to fake results — you can't simultaneously fool runners in Singapore, Frankfurt, and São Paulo.

### Token economics (simplified)

```
Website owner deposits tokens → funds credit_balance_checks
Runner completes a check      → earns 0.001–0.002 tokens off-chain
Runner balance >= 10 tokens   → automatic on-chain settlement via Anchor
Runner claims from vault PDA  → SPL tokens transferred to their wallet
```

---

## 🏗 Architecture

### System layers

```
┌─────────────────────────────────────────────────────────────────────┐
│                   PUBLIC CLIENT LAYER (BLUE)                        │
│                                                                     │
│   Rust CLI Miner (Tokio + tonic)  ←──────→  Target Websites        │
│   - Async HTTP checks                        HTTP/HTTPS endpoints   │
│   - ed25519 result signing                                          │
│   - Persistent gRPC stream                                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │  gRPC :50051 (bidirectional stream)
                             │  REST :8080  (result submission)
┌────────────────────────────▼────────────────────────────────────────┐
│                   INGRESS GATEWAY LAYER (PURPLE)                    │
│                                                                     │
│   Go gRPC Server                    Go Gin/Chi REST API             │
│   - Persistent miner streams        - /api/v1/results               │
│   - Job fan-out to miners           - /api/v1/monitors              │
│   - BatchAck per result set         - /api/v1/runner                │
│                                     - /api/v1/rewards               │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                 MESSAGE ORCHESTRATION LAYER (ORANGE)                │
│                                                                     │
│                      RabbitMQ AMQP Cluster                          │
│                                                                     │
│   ┌──────────────┐   ┌─────────────────────┐   ┌────────────────┐  │
│   │  job_queue   │   │  processing_queue   │   │solana_sync_    │  │
│   │  Job dispatch│   │  Raw result packets │   │queue           │  │
│   │  to miners   │   │  pending validation │   │Pending syncs   │  │
│   └──────────────┘   └─────────────────────┘   └────────────────┘  │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                  STORAGE & WORKER LAYER (GREEN)                     │
│                                                                     │
│   Redis ZSET Scheduler          PostgreSQL Cluster                  │
│   - Time-based URL queue        - Users, monitors, runners          │
│   - 30s monitor cache           - Partitioned ping_logs             │
│   - Nonce store (anti-cheat)    - Reward balances                   │
│   - Rate limit counters         - Solana sync events (idempotency)  │
│   - IP abuse detection                                              │
│                                                                     │
│   Go Worker Pool                                                    │
│   - Result processor (pgx.CopyFrom bulk insert)                    │
│   - Reward accumulator (atomic PL/pgSQL function)                   │
│   - Solana sync handler (threshold-triggered settlement)            │
│   - Partition cron (weekly ping_logs table creation)               │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                     BLOCKCHAIN LAYER (RED)                          │
│                                                                     │
│              Solana Mainnet / Anchor Framework                      │
│              SPL Token Reward Contract                              │
│              Reward Vault PDA                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Request flow — miner submits results

```
[1]  Redis Scheduler polls due monitors every second
[2]  Scheduler acquires per-region Redis lock (prevents duplicate dispatch)
[3]  Job published to job_queue with signed nonce (job_id)
[4]  gRPC server consumes job_queue → pushes JobBatch to connected miner stream
[5]  Rust miner executes concurrent HTTP checks via Tokio
[6]  Miner sends signed PingResultBatch back up the gRPC stream
[7]  Server validates each result's job_id (atomic Redis Lua GET+DEL)
[8]  Valid batch published to processing_queue
[9]  Result processor bulk-inserts via pgx.CopyFrom (1000 rows/batch)
[10] Atomic PL/pgSQL accumulate_runner_reward() updates off-chain balance
[11] If balance >= threshold → publishes to solana_sync_queue
[12] Solana sync worker calls Anchor add_reward instruction
[13] Confirmed tx_signature recorded in solana_sync_events (idempotency)
[14] Runner's pending_solana_sync flag cleared
```

### Dependency injection map

```
main.go
 └── app.New(cfg)
      ├── pgxpool          ─────────────────┐
      ├── redis.Client     ──────────────┐  │
      └── amqp.Channel     ───────────┐  │  │
                                      │  │  │
      └── repositories.NewStorage(pool) ◄─┘ │
           ├── UserRepository                │
           ├── MonitorRepository             │
           ├── RunnerRepository              │
           ├── PingLogRepository             │
           └── SolanaSyncRepository          │
                                             │
      └── services                           │
           ├── UserService(store, cfg)        │
           ├── MonitorService(store, rdb, ch) ◄──┘
           ├── RunnerService(store, rdb, ch)
           ├── RewardService(store, ch, cfg)
           └── PingLogService(store, pool)
                                      │
      └── controllers (one per service)
      └── router.New(cfg, ...controllers)
      └── grpc.NewServer(runnerSvc, monitorSvc)
      └── workers.Start*(ctx, ...)
```

---

## 📁 Project Structure

```
depin-backend/
│
├── main.go                          # Entrypoint — loads config, calls app.New()
│
├── app/
│   └── application.go               # DI root — wires every layer, starts both servers
│
├── config/
│   ├── db/db.go                     # pgxpool with connection limits + health checks
│   └── env/env.go                   # godotenv loader, mustGetEnv, typed Config struct
│
├── proto/
│   └── miner.proto                  # Source of truth for gRPC wire contract
│
├── buf.yaml                         # buf module config
├── buf.gen.yaml                     # Code generation targets (Go + grpc plugins)
│
├── grpc/
│   ├── pb/                          # Generated — DO NOT EDIT
│   │   ├── miner.pb.go              # Message structs
│   │   └── miner_grpc.pb.go        # Service interface + stream types
│   └── server.go                    # MinerServiceServer implementation
│
├── router/
│   └── router.go                    # Chi router — all routes, CORS, JWT middleware mount
│
├── middleware/
│   └── jwt.go                       # HS256 JWT validation, context injection
│
├── controllers/
│   ├── user.go                      # POST /auth/register, POST /auth/login
│   ├── monitor.go                   # CRUD + stats for website monitors
│   └── ping.go                      # POST /results (miner submission), runner + reward ctrl
│
├── dto/
│   └── dto.go                       # All request/response structs — single source of truth
│
├── services/
│   ├── user_service.go              # Registration, login, JWT issuance, bcrypt
│   ├── monitor_service.go           # Monitor CRUD + Redis cache invalidation
│   ├── runner_service.go            # Node registration, heartbeat, geo lookup
│   ├── reward_service.go            # Atomic accumulation + solana_sync_queue trigger
│   └── ping_log_service.go          # Bulk insert helper, ResultPacket marshalling
│
├── db/
│   └── repositories/
│       ├── storage.go               # Storage struct, all repo interfaces, domain models
│       ├── users.go                 # userRepo implementation
│       ├── monitors.go              # monitorRepo implementation
│       └── runners.go               # runnerRepo + pingLogRepo + solanaSyncRepo
│
├── workers/
│   ├── scheduler.go                 # Redis ZSET scheduler — dispatches jobs every second
│   ├── result_processor.go          # Consumes processing_queue → bulk insert + reward
│   ├── solana_sync.go               # Consumes solana_sync_queue → Anchor add_reward
│   └── partition_cron.go            # Creates next 4 weekly ping_logs partitions
│
├── anticheat/
│   └── validator.go                 # Nonce Lua GET+DEL, rate limit, IP abuse, fake latency
│
├── solana/
│   └── client.go                    # JSON-RPC wrapper — wire in solana-go/anchor-go here
│
├── docker-compose.yml               # Local infra: Postgres 16, Redis 7, RabbitMQ 3.13
├── Makefile                         # make infra / dev / build / proto / migrate
├── .env.example                     # All required environment variables with comments
└── PROTO_GENERATION.md              # buf + protoc generation guide with Rust tonic notes
```

---

## ⚡ Quick Start

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Go | 1.22+ | [go.dev/dl](https://go.dev/dl) |
| Docker + Compose | latest | [docker.com](https://docker.com) |
| buf | latest | `brew install bufbuild/buf/buf` |
| make | any | pre-installed on macOS/Linux |

### Boot in 4 commands

```bash
# 1. Clone and enter
git clone https://github.com/everestp/depin-backend && cd depin-backend

# 2. Start Postgres, Redis, RabbitMQ
make infra

# 3. Configure environment
cp .env.example .env
# Edit .env — at minimum set JWT_SECRET and DATABASE_URL

# 4. Apply schema + run
make migrate
make dev
```

The backend is now running:
- **HTTP REST** → `http://localhost:8080`
- **gRPC** → `localhost:50051`
- **RabbitMQ UI** → `http://localhost:15672` (guest / guest)

### Generate proto stubs (first time or after proto changes)

```bash
make proto
# generates grpc/pb/miner.pb.go + grpc/pb/miner_grpc.pb.go
```

---

## ⚙️ Configuration

All configuration is loaded from environment variables via `config/env/env.go`. Copy `.env.example` to `.env` and fill in the required values.

| Variable | Required | Default | Description |
|---|---|---|---|
| `PORT` | | `8080` | HTTP server port |
| `GRPC_PORT` | | `50051` | gRPC server port |
| `DATABASE_URL` | ✅ | — | PostgreSQL DSN (`postgres://user:pass@host:5432/db`) |
| `REDIS_ADDR` | | `localhost:6379` | Redis address |
| `REDIS_PASSWORD` | | `` | Redis password (empty = no auth) |
| `REDIS_DB` | | `0` | Redis logical database index |
| `RABBITMQ_URL` | ✅ | — | AMQP DSN (`amqp://guest:guest@localhost:5672/`) |
| `JWT_SECRET` | ✅ | — | HS256 signing secret — min 64 random hex chars |
| `SOLANA_RPC_URL` | | mainnet public | Use Helius/Triton/QuickNode in production |
| `ADMIN_WALLET_PATH` | ✅ | — | Path to authority keypair JSON (use HSM in prod) |
| `REWARD_THRESHOLD` | | `10.0` | Off-chain token threshold that triggers settlement |

Generate a secure `JWT_SECRET`:

```bash
openssl rand -hex 64
```

---

## 📡 API Reference

All routes are prefixed `/api/v1`. Protected routes require `Authorization: Bearer <token>`.

### Auth

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/auth/register` | — | Register user + wallet |
| `POST` | `/api/v1/auth/login` | — | Login, receive JWT |

**Register** `POST /api/v1/auth/register`

```json
// Request
{
  "email": "runner@example.com",
  "password": "supersecret123",
  "wallet_pubkey": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
}

// Response 201
{
  "token": "eyJhbGci...",
  "user": {
    "id": 1,
    "email": "runner@example.com",
    "wallet_pubkey": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
  }
}
```

**Login** `POST /api/v1/auth/login`

```json
// Request
{ "email": "runner@example.com", "password": "supersecret123" }

// Response 200
{ "token": "eyJhbGci...", "user": { ... } }
```

---

### Monitors (Website Owners)

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/monitors` | ✅ | Create a new monitor |
| `GET` | `/api/v1/monitors` | ✅ | List all monitors for the caller |
| `GET` | `/api/v1/monitors/:id/stats` | ✅ | Uptime %, latency, recent pings |
| `PUT` | `/api/v1/monitors/:id/pause` | ✅ | Pause monitoring |
| `PUT` | `/api/v1/monitors/:id/resume` | ✅ | Resume monitoring |
| `DELETE` | `/api/v1/monitors/:id` | ✅ | Soft-delete a monitor |

**Create Monitor** `POST /api/v1/monitors`

```json
// Request
{
  "target_url": "https://api.mysite.com/health",
  "interval_seconds": 60
}

// Response 201
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "target_url": "https://api.mysite.com/health",
  "interval_seconds": 60,
  "credit_balance_checks": 0,
  "total_spent_tokens": 0,
  "is_active": true
}
```

**Monitor Stats** `GET /api/v1/monitors/:id/stats`

```json
// Response 200
{
  "monitor_id": "550e8400-...",
  "uptime_pct_24h": 99.87,
  "uptime_pct_7d": 99.42,
  "recent_pings": [
    {
      "runner_pubkey": "7xKXtg...",
      "latency_ms": 142,
      "status_code": 200,
      "geo_region": "eu-west",
      "timestamp": "2025-05-19T09:00:00Z"
    }
  ]
}
```

---

### Runner (Node Operators)

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/runner/register` | ✅ | Register a node runner |
| `GET` | `/api/v1/runner/me?pubkey=` | ✅ | Get runner info + balance |
| `POST` | `/api/v1/runner/heartbeat?pubkey=` | ✅ | Update last_seen_timestamp |

**Register Runner** `POST /api/v1/runner/register`

```json
// Request
{
  "owner_pubkey": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "region": "eu-west",
  "latitude": 52.3676,
  "longitude": 4.9041
}

// Response 201
{
  "id": 42,
  "owner_pubkey": "7xKXtg...",
  "region": "eu-west",
  "offchain_accumulated_tokens": 0.0000,
  "total_earned_tokens_all_time": 0.0000,
  "pending_solana_sync": false
}
```

---

### Results (Rust Miner Submission)

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/results` | ✅ | Submit a batch of ping results |

**Submit Results** `POST /api/v1/results`

```json
// Request
{
  "runner_pubkey": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "signature": "base64_ed25519_sig_over_sha256_of_results",
  "results": [
    {
      "job_id": "550e8400-...:eu-west:1716110400",
      "monitor_id": "550e8400-e29b-41d4-a716-446655440000",
      "latency_ms": 142,
      "status_code": 200,
      "geo_region": "eu-west"
    }
  ]
}

// Response 202
{ "message": "results queued" }
```

> The endpoint returns `202 Accepted` immediately. Validation and DB writes happen asynchronously in the worker pool. This keeps the miner's submission path sub-millisecond.

---

### Rewards

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/api/v1/rewards/status?pubkey=` | ✅ | Off-chain balance + sync status |

```json
// Response 200
{
  "runner_pubkey": "7xKXtg...",
  "offchain_accumulated_tokens": 7.4120,
  "total_earned_all_time": 47.2310,
  "pending_sync": false
}
```

---

### Health

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | — | Liveness probe — returns `{"status":"ok"}` |

---

## 🔌 gRPC / Proto

The Rust miner connects via a persistent bidirectional gRPC stream. See [`proto/miner.proto`](proto/miner.proto) and [`PROTO_GENERATION.md`](PROTO_GENERATION.md) for full details.

### Stream protocol

```
Miner dials :50051
  │
  ├─► ConnectRequest { runner_pubkey, region, version }
  │◄─ ConnectAck    { accepted: true, heartbeat_interval_seconds: 30 }
  │
  │  [ongoing]
  │◄─ JobBatch      { jobs: [{ job_id, target_url, expires_at, ... }] }
  ├─► PingResultBatch { runner_pubkey, signature, results: [...] }
  │◄─ BatchAck      { accepted: N, rejected: 0 }
  │
  ├─► HeartbeatRequest { sent_at: unix_ts }
  │  (every 30s — server updates last_seen_timestamp)
  │
  │◄─ DisconnectReason { reason: "server restart" }  (graceful shutdown)
```

### Generate stubs

```bash
# Install buf once
brew install bufbuild/buf/buf

# Generate Go stubs → grpc/pb/
make proto

# For Rust (in the CLI repo) — add to build.rs:
# tonic_build::compile_protos("../../proto/miner.proto").unwrap();
```

---

## 🗄 Database Schema

### Tables

| Table | Purpose | Notes |
|---|---|---|
| `users` | Website owners and runner operators | Unique wallet constraint at DB level |
| `monitors` | URLs being monitored | Soft delete, credit balance tracking |
| `runner_nodes` | Registered miner nodes | Off-chain reward balance, geo coords |
| `ping_logs` | Time-series ping results | RANGE partitioned weekly, pgx.CopyFrom bulk insert |
| `solana_sync_events` | Confirmed on-chain settlements | Idempotency — unique tx_signature |

### Partitioning strategy

`ping_logs` is range-partitioned by `timestamp` into weekly tables:

```sql
-- Auto-created every Monday by workers/partition_cron.go
ping_logs_2025_w21   -- 2025-05-19 → 2025-05-26
ping_logs_2025_w22   -- 2025-05-26 → 2025-06-02
-- 4 weeks always pre-created ahead
```

Each partition gets its own index:

```sql
CREATE INDEX ON ping_logs_2025_w21 (monitor_id, timestamp DESC);
```

### Key atomic function

`accumulate_runner_reward(pubkey, delta, threshold)` — single DB round-trip that:
1. Adds `delta` to `offchain_accumulated_tokens` and `total_earned_tokens_all_time`
2. If balance >= threshold, decrements by threshold and returns `did_sync=true`
3. Overflow remainder is preserved — runners never lose micro-earnings

```sql
SELECT new_balance, did_sync FROM accumulate_runner_reward($1, $2, $3);
```

---

## ⚙️ Background Workers

Four workers start automatically in `app.New()`:

### 1. Redis Scheduler (`workers/scheduler.go`)

- Ticks every **1 second**
- Reads active monitors from Redis cache (30s TTL, invalidated on CRUD)
- For each due monitor, acquires a per-region Redis lock (`SET NX EX`)
- Publishes `JobPayload` to `job_queue` and stores the nonce in Redis with 2× interval TTL

### 2. Result Processor (`workers/result_processor.go`)

- Consumes `processing_queue`
- Calculates reward delta per result (+0.001 base, +0.001 bonus for 2xx/3xx)
- Calls `accumulate_runner_reward()` atomic DB function
- If threshold crossed → publishes to `solana_sync_queue`
- On failure: `Nack(requeue=true)` — message retried automatically

### 3. Solana Sync Handler (`workers/solana_sync.go`)

- Consumes `solana_sync_queue`
- Checks `solana_sync_events` for existing tx signature (idempotency guard)
- Calls Anchor `add_reward` instruction via JSON-RPC
- Records confirmed `tx_signature` in DB
- Clears `pending_solana_sync` flag on runner
- On RPC failure: `Nack(requeue=true)` — retried with backoff by RabbitMQ

### 4. Partition Cron (`workers/partition_cron.go`)

- Runs on **startup** (covers missed weeks) then every **Monday 00:05 UTC**
- Creates the next 4 weekly `ping_logs_YYYY_wNN` partitions if they don't exist
- Creates indexes on each new partition
- Prevents the production incident where inserts fail because a partition doesn't exist

---

## 🛡 Anti-Cheat System

All checks live in `anticheat/validator.go`:

### 1. Nonce validation (atomic Lua GET+DEL)

Every job dispatched includes a `job_id` nonce stored in Redis with TTL = 2× check interval. On result submission, a Lua script atomically gets and deletes the nonce:

```lua
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
redis.call("DEL", KEYS[1])
return 1
```

- Expired nonces → rejected (prevents submitting after TTL)
- Consumed nonces → rejected (prevents submitting same job twice)
- Missing nonces → rejected (prevents fabricated results)

### 2. Per-runner rate limiting

Redis `INCR` + `EXPIRE` pipeline enforces max submissions per minute per runner. Prevents flooding even with valid nonces.

### 3. Same-IP multi-node abuse detection

`SADD ip:<ip> <pubkey>` with 24h TTL. If >N distinct pubkeys from one IP → `ErrIPAbuse`. Detects Sybil attacks where one machine pretends to be many geographically diverse nodes.

### 4. Fake latency streak detection

If the same `latency_ms` value is seen >10 times in 5 minutes from the same runner, `ErrSuspiciousLatency` is returned. Real network variance means identical latency across many requests is statistically impossible.

### 5. ed25519 result signing (architecture)

The Rust miner signs each `PingResultBatch` with its private key. The backend verifies the signature against the registered `owner_pubkey` before accepting results. Prevents one runner submitting fake results on behalf of another.

---

## 💰 Reward Engine

### Off-chain accounting (high frequency)

Every ping result updates the runner's `offchain_accumulated_tokens` in Postgres via the atomic `accumulate_runner_reward()` PL/pgSQL function. This handles thousands of updates per second without any blockchain calls.

```
Runner completes check → +0.001 tokens (base)
                       → +0.001 tokens (bonus for 2xx/3xx response)
```

### On-chain settlement (threshold triggered)

When `offchain_accumulated_tokens >= REWARD_THRESHOLD` (default: 10 tokens):

1. DB atomically decrements by threshold, preserves overflow remainder
2. Sync job published to `solana_sync_queue`
3. Solana sync worker calls `add_reward` on the Anchor program
4. Anchor program credits the runner's `NodeAccount.reward_balance`
5. Confirmed `tx_signature` recorded — prevents double-credit on RPC retry

### Runner claims (frontend)

The Next.js dashboard calls `claim_reward` on the Anchor program directly from the user's Phantom wallet. The smart contract transfers SPL tokens from the reward vault PDA to the runner's wallet.

### Token decimal handling

```
$UPT has 6 decimal places
1 token = 1_000_000 raw units

// Always work in raw units on-chain:
amount_raw = int64(amount_tokens * 1_000_000)
```

---

## ⛓ Solana Integration

### Smart contract (Anchor)

The Go backend interacts with two Anchor instructions:

| Instruction | Caller | Description |
|---|---|---|
| `init_node` | Go backend | Creates runner's PDA on first registration |
| `add_reward` | Go backend (ADMIN_WALLET) | Credits `reward_balance` on runner's NodeAccount |
| `claim_reward` | Runner wallet (frontend) | Transfers SPL from vault PDA to runner wallet |

### NodeAccount PDA

```rust
// Seed: ["node", owner_pubkey, email_hash]
// Space: 8 (discriminator) + 32 + 32 + 8 + 1 = 81 bytes
pub struct NodeAccount {
    pub owner_pub_key: Pubkey,   // 32 bytes
    pub email_hash: [u8; 32],    // 32 bytes — sha256(email)
    pub reward_balance: u64,     // 8 bytes — raw token units
    pub bump: u8,                // 1 byte
}
```

### Security — add_reward authority check

```rust
pub fn add_reward(ctx: Context<AddReward>, amount: u64) -> Result<()> {
    require!(
        ctx.accounts.backend.key() == ADMIN_WALLET,
        ErrorCode::Unauthorized
    );
    // checked_add prevents overflow
    node.reward_balance = node.reward_balance
        .checked_add(amount)
        .ok_or(ErrorCode::Overflow)?;
    Ok(())
}
```

The `ADMIN_WALLET` check is the **first line** of `add_reward` — before any state mutation. Without it, any wallet could call `add_reward` and drain the vault.

### RPC provider

Never use the public mainnet endpoint in production. Configure a dedicated provider:

```env
# Helius (recommended)
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY

# Triton
SOLANA_RPC_URL=https://yourproject.rpcpool.com/YOUR_KEY

# QuickNode
SOLANA_RPC_URL=https://your-endpoint.solana-mainnet.quiknode.pro/YOUR_KEY
```

---

## 🔐 Security Model

### Authentication

- All REST endpoints (except `/health`, `/auth/*`) require a valid HS256 JWT
- JWT contains `sub` (user ID) and `email`, signed with `JWT_SECRET`
- Tokens expire after 72 hours
- JWT secret must be rotated quarterly via secrets manager

### Transport

- All external traffic should terminate at a TLS-terminating load balancer (Nginx, Caddy, AWS ALB)
- gRPC should run behind TLS in production (add `grpc.Creds(credentials.NewServerTLSFromCert(...))`)
- Internal service-to-service traffic (within the same VPC/cluster) can be plaintext

### Wallet uniqueness

The `wallet_pubkey` column has a `UNIQUE` constraint enforced at the **database level**. Application-level uniqueness checks alone are vulnerable to race conditions on concurrent registration.

### Admin wallet / keypair

```
❌ Never:  store raw keypair bytes in a flat file on a shared server
❌ Never:  load ADMIN_WALLET from an environment variable on a VPS

✅ Always: use HSM (Hardware Security Module) or KMS (AWS KMS, GCP KMS)
✅ Always: restrict the signing key to only the add_reward instruction
```

### JWT secret management

```
❌ Never:  hardcode JWT_SECRET in source code or a .env committed to git
✅ Always: inject from HashiCorp Vault, AWS Secrets Manager, or GCP Secret Manager
✅ Always: rotate quarterly
```

### SQL injection

All queries use pgx parameterized statements (`$1`, `$2`, ...). Raw string interpolation is not used anywhere in the codebase.

---

## 🚀 Deployment

### Docker (single node)

```dockerfile
# Build stage
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o depin-backend ./main.go

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/depin-backend /depin-backend
EXPOSE 8080 50051
ENTRYPOINT ["/depin-backend"]
```

```bash
docker build -t depin-backend:latest .
docker run -p 8080:8080 -p 50051:50051 --env-file .env depin-backend:latest
```

### Kubernetes (production)

Recommended topology:

```
Deployment: depin-backend   (2+ replicas, HPA on CPU/RPS)
  → Service: ClusterIP :8080, :50051
  → Ingress: nginx / ALB with TLS termination

StatefulSet: rabbitmq       (3-node cluster for HA)
StatefulSet: redis          (Redis Sentinel or Redis Cluster)
StatefulSet: postgres       (Primary + 1 replica, PgBouncer sidecar)
```

Key K8s settings for this workload:

```yaml
resources:
  requests:
    cpu: "500m"
    memory: "256Mi"
  limits:
    cpu: "2"
    memory: "1Gi"

livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15

readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Database migrations

Apply the schema before the first deploy:

```bash
make migrate
# equivalent to:
psql "$DATABASE_URL" -f schema.sql
```

Use a migration tool (Goose, Flyway, Atlas) for subsequent schema changes — never alter production tables by hand.

### Graceful shutdown

The application handles `SIGINT` and `SIGTERM`:

1. HTTP server: `Shutdown(ctx)` with 30s timeout — drains in-flight requests
2. gRPC server: `GracefulStop()` — waits for active streams to finish
3. Workers: `ctx.Done()` propagation — consumers stop after current message

Kubernetes `terminationGracePeriodSeconds` should be set to **45s** to give the app time to drain before the pod is killed.

---

## 📊 Observability

### Recommended stack

| Tool | Purpose |
|---|---|
| **Prometheus** | Metrics scraping |
| **Grafana** | Dashboards |
| **Loki** | Log aggregation |
| **Jaeger / Tempo** | Distributed tracing |
| **PgBouncer** | Postgres connection pooling |
| **RabbitMQ Management** | Queue depth monitoring |

### Key metrics to expose

Add these via `prometheus/client_golang`:

```go
// Queue depths
depin_job_queue_depth
depin_processing_queue_depth
depin_solana_sync_queue_depth

// Miner connections
depin_grpc_connected_miners_total

// Results
depin_ping_results_accepted_total
depin_ping_results_rejected_total{reason="expired_nonce|rate_limit|ip_abuse|fake_latency"}

// Rewards
depin_rewards_accumulated_tokens_total
depin_solana_syncs_total{status="success|failure"}
depin_solana_sync_latency_seconds

// DB
depin_bulk_insert_rows_total
depin_db_pool_acquired_connections
```

### Structured logging

Replace `log.Printf` with `slog` (Go 1.21+):

```go
slog.Info("miner connected",
    "pubkey", pubkey,
    "region", region,
    "version", version,
    "connected_total", s.ConnectedCount(),
)
```

### Alerting rules (Prometheus)

```yaml
# Alert if no miners have been connected for 5 minutes
- alert: NoMinersConnected
  expr: depin_grpc_connected_miners_total == 0
  for: 5m

# Alert if solana sync queue is backing up
- alert: SolanaSyncQueueHigh
  expr: depin_solana_sync_queue_depth > 100
  for: 2m

# Alert if rejection rate spikes (potential attack)
- alert: HighRejectionRate
  expr: rate(depin_ping_results_rejected_total[5m]) > 50
  for: 1m
```

---

## 🧑‍💻 Development Guide

### Run tests

```bash
go test ./...                    # all tests
go test ./services/... -v        # verbose service tests
go test ./anticheat/... -race    # race detector on anti-cheat
```

### Lint

```bash
# Install golangci-lint
brew install golangci-lint

make lint
```

### Add a new route

1. Add request/response structs to `dto/dto.go`
2. Add method to the relevant `services/` interface + implementation
3. Add handler to the relevant `controllers/` file
4. Register the route in `router/router.go`
5. No changes needed to `app/application.go` unless it's a new service

### Add a new worker

1. Create `workers/my_worker.go` with a `StartMyWorker(ctx, ...) ` function
2. Call it in `app/application.go` after the other `workers.Start*` calls
3. Declare any new RabbitMQ queues in `app.declareQueues()`

### Proto changes

1. Edit `proto/miner.proto`
2. Run `make proto` to regenerate `grpc/pb/`
3. Fix any compilation errors in `grpc/server.go`
4. Update `build.rs` in the Rust miner repo and recompile

### Makefile targets

```bash
make infra       # start Postgres, Redis, RabbitMQ via docker compose
make down        # stop local infra
make dev         # run the backend (requires infra + .env)
make build       # compile binary to bin/depin-backend
make proto       # regenerate gRPC stubs from proto/miner.proto
make migrate     # apply schema.sql to DATABASE_URL
make tidy        # go mod tidy
make lint        # golangci-lint
```

---

## ✅ Production Checklist

### Before launch

- [ ] Generate `JWT_SECRET` with `openssl rand -hex 64` and store in secrets manager
- [ ] Store `ADMIN_WALLET` keypair in HSM/KMS — never a flat file
- [ ] Configure dedicated Solana RPC (Helius / Triton / QuickNode)
- [ ] Fund the reward vault SPL token account before opening to runners
- [ ] Run a formal smart contract audit on the Anchor program
- [ ] Pre-create 8 weeks of `ping_logs` partitions (`make migrate`)
- [ ] Set up TLS termination on the load balancer for `:8080` and `:50051`
- [ ] Configure RabbitMQ dead-letter exchange for permanently failed messages
- [ ] Enable Postgres `pg_stat_statements` for query performance monitoring
- [ ] Set `terminationGracePeriodSeconds: 45` in your K8s Deployment

### Security

- [ ] Confirm `require!(backend.key() == ADMIN_WALLET)` is the **first** line of `add_reward`
- [ ] Confirm `wallet_pubkey` has `UNIQUE` constraint at DB level (not just app level)
- [ ] Rotate JWT secret quarterly
- [ ] Restrict RabbitMQ management UI to internal network only
- [ ] Ensure Redis is not publicly accessible
- [ ] Enable Postgres SSL (`sslmode=require` in `DATABASE_URL`)

### Performance

- [ ] Test bulk insert throughput: `pgx.CopyFrom` should handle 10k rows/s on a `db.t3.medium`
- [ ] Verify Redis ZSET scheduler handles 10,000 active monitors (benchmark the cache hit rate)
- [ ] Load test gRPC stream with 1,000 concurrent miner connections
- [ ] Confirm partition cron runs successfully and partitions have indexes

### Observability

- [ ] Prometheus metrics endpoint `/metrics` is exposed and scraped
- [ ] Grafana dashboard shows queue depths, connected miners, sync latency
- [ ] Alerting rules deployed for: no miners connected, sync queue backup, high rejection rate
- [ ] Loki log aggregation collecting structured logs from all pods

---

## 🚧 Known Limitations & TODOs

| Area | Status | Notes |
|---|---|---|
| **Solana client** | 🔧 Stub | Wire in `github.com/gagliardetto/solana-go` for real transaction signing |
| **ed25519 verification** | 🔧 TODO | Verify miner's `signature` field in `processResultBatch` |
| **gRPC region routing** | 🔧 TODO | Replace `break` in `ConsumeJobQueue` with geo-aware miner selection |
| **Anti-cheat wiring** | 🔧 TODO | Call `anticheat.Validator` inside `processResultBatch` per result |
| **Proto stubs** | 📋 Generated | Run `make proto` — stubs not committed until after first generation |
| **Metrics** | 📋 TODO | Add `prometheus/client_golang` instrumentation |
| **Structured logging** | 📋 TODO | Replace `log.Printf` with `slog` |
| **DB migrations** | 📋 TODO | Adopt Goose or Atlas for versioned schema changes |
| **gRPC TLS** | 📋 TODO | Add `grpc.Creds(...)` for production mTLS between miner and backend |
| **Rate limit config** | 📋 TODO | Expose `maxPerMinute` via environment variable |

---

## 📄 License

MIT — see [LICENSE](LICENSE).

---

<div align="center">

Built with Go · Solana · RabbitMQ · Redis · PostgreSQL · gRPC

</div>
