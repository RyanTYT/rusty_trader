# rusty_trader
 
A self-hosted live algorithmic trading system written in Rust, interfacing with Interactive Brokers (IBKR) via TWS through the `rust-ibapi` crate.
 
The public repository contains the system skeleton — architecture, database schema, order engine, and broker abstraction — without the specific strategies currently under live testing.
 
## Repositories
 
| Repo | Description |
|------|-------------|
| [`rusty_trader`](https://github.com/RyanTYT/rusty_trader) | Rust backend: OMS, strategies, DB, broker layer |
| [`rusty_trader_front`](https://github.com/RyanTYT/rusty_trader_front) | Tauri frontend: performance dashboard, position viewer |
 
---
 
## Architecture
 
```
Strategies (per Tokio task)
    └── Consolidator          — unified market data subscriptions, auto-resubscribe
    └── OrderEngine           — SELL → FX → BUY chains, unified OMS event loop
        └── IBKR / TWS        — rust-ibapi (primary price + order execution)
        └── yfinance fallback — TSE (.T), LSE (.L), KRX suffix mapping
    └── PostgreSQL (sqlx)     — positions, orders, fills, CASH:{currency} ledger rows
```
 
**Threading model:** Each strategy runs as a dedicated Tokio task. Shared state is guarded with `std::sync::Mutex` or `tokio::sync::Mutex` depending on whether the critical section crosses an `.await` point.
 
---
 
## Key Components
 
### OrderEngine
Handles the full order lifecycle. Multi-currency rebalancing uses SELL→FX→BUY attachment chains: each leg is spawned only after the prior leg is confirmed filled. FX orders are tagged via `orderRef` for attribution back to the originating position. A unified OMS event loop processes all open order events across every active strategy.
 
### Consolidator
Abstracts IBKR market data subscriptions. Multiple strategies interested in the same contract share a single active subscription. Handles re-subscription automatically when data goes stale, and emits a unified trigger per contract regardless of how many strategies are listening.
 
### Dynamic CRUD (Proc Macros)
Models are declared once as structs. Two proc macros handle the rest:
- `#[derive(CrudKeys)]` — generates primary key and composite key types at compile time
- A second proc macro generates `sqlx` query trait implementations with compile-time
  type checking via the `sqlx::query!` macro

### Price Fetching
IBKR TWS is the primary source. Fallback to yfinance on failure, with Yahoo Finance exchange-suffix mapping:
 
| Exchange | Yahoo Suffix |
|----------|-------------|
| TSE (Tokyo) | `.T` |
| LSE (London) | `.L` |
| KRX (Korea) | handled separately |
 
### Database Schema (abbreviated)
 
```sql
-- Positions (multi-currency, including cash rows)
positions (id, ticker, exchange, currency, quantity, avg_cost, updated_at)
-- CASH:{currency} rows (e.g. CASH:SGD) track per-currency cash balances
 
-- Orders
orders (id, order_ref, ticker, action, quantity, order_type, status, strategy, created_at)
 
-- Fills
fills (id, order_id, fill_price, fill_quantity, commission, filled_at)
```
 
---
 
## Getting Started
 
### Prerequisites
- Rust (stable toolchain)
- Docker + Docker Compose
- Interactive Brokers TWS or IB Gateway running locally, with API enabled on port 7497
- `.env` file with database credentials and IBKR connection config

### Running
 
```bash
# Start PostgreSQL and any auxiliary services
docker compose up -d
 
# Run the backend
cargo run --release
```
 
Strategies are loaded at startup. Each registered strategy is spawned onto a dedicated
OS thread and begins subscribing to its required contracts via the Consolidator.
 
### Running the Frontend
 
See [`rusty_trader_front`](https://github.com/RyanTYT/rusty_trader_front) for the Tauri
desktop/mobile dashboard.
 
---
 
## Tech Stack
 
| Component | Technology |
|-----------|-----------|
| Language | Rust (stable) |
| Async runtime | tokio |
| IBKR interface | rust-ibapi |
| Database | PostgreSQL |
| DB query layer | sqlx (compile-time checked) |
| Price fallback | yfinance (via subprocess) |
| Deployment | Docker Compose |
| Frontend | React + TypeScript + Tauri |
 
---
 
## Notes
 
- The public repository is a **skeleton**. Strategies under active live testing are not
  committed to the public repo.
- Base currency: SGD (IBKR account). All multi-currency positions go through FX legs
  managed by the OrderEngine.
- The system is designed to be broker-agnostic at the trait level — the `Broker` trait
  exposes `get_current_price`, `start_live_strategy`, and order placement methods, making
  a future port to a different broker a matter of implementing the trait rather than
  rewriting core logic.
---


If one has no permission to get prices or the contract is invalid - for e.g. with Option contracts, get_current_price can hang for up to around 30~45s. Multiply that by a few option contracts and using PendingDbQuery as the bar update outcome with this can very easily exceed the 1 minute threshold set by the FOREX contract subscribed to by unknown/manual/any strategy you build that takes 1 minute bars.

In a nutshell, if you encounter "Consumer Stall" error logs, you should first check hotpath-console to see which thread is blocking, and consequently which function in that thread is blocking, then read the logs as well; typically, some consumer thread IS stalling for some reason, typically having to do with get_current_price or an IBKR client side sync function.
