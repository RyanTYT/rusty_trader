# Trading Bot Backend (Rust)

This backend exposes a set of REST API routes to manage and monitor a trading system.  
It is built in **Rust** with a focus on clarity, safety, and extensibility.

---

## 📡 API Routes

### 🔔 Notifications
- **POST** `/send_notification` → Send a general notification.
- **POST** `/send/positions_mismatch` → Trigger a mismatch alert between target and current positions.
- **POST** `/current_position/fix` → Reconcile and fix current positions.

---

### 📊 Portfolio
- **GET** `/get_portfolio/strategy` → Get portfolio value for a specific strategy.
- **GET** `/get_portfolio` → Get overall portfolio value across all strategies.

---

### ⚙️ Strategy & Account Control
- **POST** `/strategy/pause` → Pause a strategy.
- **POST** `/strategy/resume` → Resume a strategy.
- **POST** `/account/pause` → Pause the entire trading account.

---

### 📈 Strategy Management (CRUD)
- **POST** `/strategy` → Create a strategy.
- **GET** `/strategy` → Read details of a strategy.
- **GET** `/strategy/all` → Read all strategies.
- **PUT** `/strategy` → Update a strategy.
- **DELETE** `/strategy` → Delete a strategy.

---

### 📝 Logs
- **GET** `/logs` → List available log files.
- **GET** `/logs/:filename` → Read a specific log file.

---

### 📦 Current Stock Positions (CRUD)
- **POST** `/current_stock_positions` → Create entry.
- **GET** `/current_stock_positions` → Read entry.
- **GET** `/current_stock_positions/all` → Read all entries.
- **PUT** `/current_stock_positions` → Update entry.
- **DELETE** `/current_stock_positions` → Delete entry.

---

### 📦 Current Option Positions (CRUD)
- **POST** `/current_option_positions` → Create entry.
- **GET** `/current_option_positions` → Read entry.
- **GET** `/current_option_positions/all` → Read all entries.
- **PUT** `/current_option_positions` → Update entry.
- **DELETE** `/current_option_positions` → Delete entry.

---

### 🎯 Target Stock Positions (CRUD)
- **POST** `/target_stock_positions` → Create entry.
- **GET** `/target_stock_positions` → Read entry.
- **GET** `/target_stock_positions/all` → Read all entries.
- **PUT** `/target_stock_positions` → Update entry.
- **DELETE** `/target_stock_positions` → Delete entry.

---

### 🎯 Target Option Positions (CRUD)
- **POST** `/target_option_positions` → Create entry.
- **GET** `/target_option_positions` → Read entry.
- **GET** `/target_option_positions/all` → Read all entries.
- **PUT** `/target_option_positions` → Update entry.
- **DELETE** `/target_option_positions` → Delete entry.

---

### 📝 Open Stock Orders (CRUD)
- **POST** `/open_stock_orders` → Create entry.
- **GET** `/open_stock_orders` → Read entry.
- **GET** `/open_stock_orders/all` → Read all entries.
- **PUT** `/open_stock_orders` → Update entry.
- **DELETE** `/open_stock_orders` → Delete entry.

---

### 📝 Open Option Orders (CRUD)
- **POST** `/open_option_orders` → Create entry.
- **GET** `/open_option_orders` → Read entry.
- **GET** `/open_option_orders/all` → Read all entries.
- **PUT** `/open_option_orders` → Update entry.
- **DELETE** `/open_option_orders` → Delete entry.

---

### 💸 Stock Transactions (CRUD)
- **POST** `/stock_transactions` → Create entry.
- **GET** `/stock_transactions` → Read entry.
- **GET** `/stock_transactions/all` → Read all entries.
- **PUT** `/stock_transactions` → Update entry.
- **DELETE** `/stock_transactions` → Delete entry.

---

### 💸 Option Transactions (CRUD)
- **POST** `/option_transactions` → Create entry.
- **GET** `/option_transactions` → Read entry.
- **GET** `/option_transactions/all` → Read all entries.
- **PUT** `/option_transactions` → Update entry.
- **DELETE** `/option_transactions` → Delete entry.

---

### 📜 Historical Data (CRUD)
- **POST** `/historical_data` → Create entry.
- **GET** `/historical_data` → Read entry.
- **GET** `/historical_data/all` → Read all entries.
- **PUT** `/historical_data` → Update entry.
- **DELETE** `/historical_data` → Delete entry.

---

### 📜 Historical Options Data (CRUD)
- **POST** `/historical_options_data` → Create entry.
- **GET** `/historical_options_data` → Read entry.
- **GET** `/historical_options_data/all` → Read all entries.
- **PUT** `/historical_options_data` → Update entry.
- **DELETE** `/historical_options_data` → Delete entry.

---

---

## 🚀 Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/rusty_trader.git
   cd rusty_trader
