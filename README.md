# Cunningham Worker

Execution worker for interacting with target chatbots via Telegram MTProto protocol.

## Overview

This service:
- Consumes execution jobs from RabbitMQ
- Sends messages to target bots using Telethon (MTProto)
- Receives bot responses
- Publishes execution results and session completion notices back to RabbitMQ

## Quick Start

### Prerequisites

- Python 3.11+
- Telegram API credentials (api_id, api_hash)
- RabbitMQ server

### Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

### Running

```bash
python -m cunninghamworker
```

## Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `TELEGRAM_API_ID` | Telegram API ID | Yes |
| `TELEGRAM_API_HASH` | Telegram API Hash | Yes |
| `TELEGRAM_PHONE` | Phone number for auth | Yes |
| `RABBITMQ_HOST` | RabbitMQ server host | Yes |
| `RABBITMQ_QUEUE_NAME` | Queue to consume from | Yes |
| `RABBITMQ_EXECUTION_RESULTS_QUEUE_NAME` | Queue for job results | Yes |
| `RABBITMQ_SESSION_COMPLETED_QUEUE_NAME` | Queue for session completion notices | Yes |

## Architecture

The worker follows DDD principles:
- **Domain**: Execution job entities
- **BLL**: Job processing logic
- **Infrastructure**: Telethon client, RabbitMQ consumer/publisher
- **Presentation**: None (background worker)

## License

See LICENSE file.
