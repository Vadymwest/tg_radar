# 🎯 tg_radar — Production-Grade AI-Powered Telegram Lead Generation Bot

An enterprise-ready, asynchronous Telegram userbot designed to actively scan chats, large supergroups, and channel comments to detect high-value IT leads, freelance requests, and automation projects. 

Equipped with a high-performance hybrid filtering engine (**Regex Pre-filtering + GPT-4o-mini Evaluation**), the system ensures pinpoint lead accuracy while optimizing OpenAI API consumption and operational costs.

---

## ⚡ Architectural Highlights & "Killer" Features

### 1. Active Polling Engine (Overcoming Telegram Limits)
Most open-source parsers rely on passive event listeners (`@app.on_message`). However, Telegram drops WebSocket updates for muted channels and large forum-like supergroups (100k+ users) on userbot sessions. 
`tg_radar` solves this completely by implementing **Active Polling** via a sliding window `get_chat_history` logic. It dynamically polls channels, ensuring **zero missed leads** regardless of chat notification settings.

### 2. High-Performance Hybrid Filtering Pipeline
To optimize server costs and protect your OpenAI budget, traffic passes through an immediate, non-blocking two-stage evaluation:
* **Stage 1 (Local Regex Matching):** A highly optimized keyword engine built using complex regex lookarounds (`(?<!...)` and `(??!...)`) to eliminate false-positive homonyms (e.g., matches `api` or `p2p` as standalone tokens but ignores words like *n**api**shi*).
* **Stage 2 (AI Gatekeeper):** Potentially qualified messages are handed off to `gpt-4o-mini` with a specialized prompt to instantly filter out channel-spam, obfuscated Unicode fonts, bounty/airdrop tasks, and low-tier micro-tasks (KYC verification, manual site registrations).

### 3. Non-Blocking I/O Workflow
The core polling loop does not wait for OpenAI API responses. Qualified messages are pushed to separate, independent asynchronous tasks (`asyncio.create_task`), completely preventing bottlenecking or task starvation across hundreds of monitored chats.

### 4. Atomic Persistence & Blacklist Optimization
The bot safeguards your session and operational memory through efficient state preservation:
* Duplication checks are handled in-memory using highly-optimized **Python Sets ($O(1)$ search complexity)**.
* Disc serialization runs batched at the end of each polling cycle, preventing disk I/O thrashing.
* State saving uses an **atomic file swap mechanism** (`write` to `.tmp` followed by OS-level `os.replace`) preventing state corruption during unexpected server crashes.
* **Smart User Behavior Emulation:** Traverses chats in a randomized shuffle pattern with variable delay handling to naturally bypass anti-fraud and rate-limiting blocks.

---

## 🛠 Tech Stack
* **Core:** Python 3.10+
* **Framework:** Pyrogram v2 (MTProto API)
* **LLM Core:** OpenAI Async API (`gpt-4o-mini`)
* **Concurrency:** Asyncio (Fully asynchronous I/O-bound execution)

---

## 📦 Production Deployment & Quick Start

### 1. Clone & Set Up Environment
```bash
git clone [https://github.com/Vadymwest/tg_radar.git](https://github.com/Vadymwest/tg_radar.git)
cd tg_radar