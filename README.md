# rusty_trader

A rust implementation of a trading bot based on the IBKR API (with rust ibapi) with fully automated executions and a frontend to track strategy performance
Check [this](https://ryantyt.github.io/RTYT/journal/summer_trading_bot_3) out for a more personal post on the building of this.
Note: If you actually want to try it out, you need the environment file in the root folder following the .env.example file and also add the stable distribution of IB gateway to the IB folder.

---

## Overview

This repository showcases the **public interface** of the trading system.  
The full implementation connects to **Interactive Brokers (IBKR)** via their API (with the help of rust ibapi), manages portfolio positions, executes trades, and supports research/backtesting.  

The goal of this repo is to showcase the:
- [**Backend architecture**](/trading-app/README.md) used for the strategies; the older Python implementation is left in for posterity.
- A separate [**Backend**](/backend/README.md) used for easy user interface built in rust as well.
- **Containerized development setup** with Docker for portability.
- The frontend architecture can be found [here](https://github.com/RyanTYT/rusty_trader_front).
- For a look at some trading strategies I backtested that have found their way into my personal implementations, have a look [here](https://github.com/RyanTYT/TradingResearch)

---

## ⚙️ Features

- **IBKR Integration**  
  - Connects to Interactive Brokers’ Trader Workstation / IB Gateway.  
  - Handles positions, orders, and execution callbacks, updating the local DB accordingly.

- **Systematic Trading Interface**  
  - Unified API for positions, target allocations, transactions, and historical data.
  - Hybrid architecture: as far as rust is a hybrid between OOP and functional programming.

- **Containerized Setup**  
  - `docker-compose.yml` to run the trading-bot with IBKR Gateway in a GUI-enabled container (enabled with Xvfb).
  - Simple tests in the tests folder to test order management and bar consolidation.

---

## 📂 Repo Structure

- IB: Builds the Docker Image to host the installed IB Gateway Instance
- Backtester: Contains a backtester module built in Rust that mirrors the execution model of the Strategy in `trading-app` - to backtest a lot more accurately taking into account commissions, bids and asks, and next bar execution - as well as additional rudimentary code related to optuna tuning of hyperparameters of certain indicator functions.
- backend: The backend to communicate with the local trading database and the frontend - for analytics and tracking of strategy performance
- postgres-data: To host the data of the timescaledb/postgresdb Database for persistence.
- trading-app: The rust trading application that is the main program to run the strategies on
- trading-app-old: The older Python implementation of the trading application for posterity (can be taken a look at for very blatant issues and problems with building such an application in Python - Loss of static typing, Bad event management system, 0 multithreading for strategies, ...)
