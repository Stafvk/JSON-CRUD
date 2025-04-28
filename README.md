# Healthcare Plan API

A distributed microservices architecture for healthcare plan data management, implementing a polyglot persistence model with Redis as primary datastore, Elasticsearch as search engine, and RabbitMQ for asynchronous event-driven communication.

## Overview

This project implements a high-throughput, fault-tolerant system for healthcare plan management leveraging a microservices architecture pattern with eventual consistency. The system employs a CQRS-inspired (Command Query Responsibility Segregation) approach, separating the write and read paths:

- **Write Path**: REST API → Redis (primary store) → RabetMQ → Elasticsearch (secondary index)
- **Read Path**: Query router → Elasticsearch (complex queries) → Redis (fallback/cache)

The architecture implements key distributed systems patterns:

- **Event-driven communication** via message queues for system resilience
- **Polyglot persistence** utilizing the strengths of different data stores
- **Optimistic concurrency control** via ETags to handle concurrent modifications
- **Command-Query Responsibility Segregation** pattern for scalability
- **Parent-child document relationships** for complex hierarchical data modeling
- **JWT token-based authentication** with asymmetric key verification
- **Circuit breaker pattern** for search with Redis fallback
- **Cascading delete operations** to maintain referential integrity

The system leverages containerization for deployment consistency and service isolation, with each component designed to scale horizontally to meet varying load requirements.

## Features

- **RESTful API** with full CRUD operations following HTTP semantics
- **JSON Schema validation** with AJV for request payload validation
- **Conditional HTTP requests** (If-Match, If-None-Match) for optimistic concurrency
- **Document-oriented data model** with parent-child relationships
- **Asynchronous event processing** for system resilience and fault tolerance
- **Elasticsearch DSL** for complex hierarchical document queries
- **Graceful degradation** with Redis fallback when Elasticsearch is unavailable
- **Stateless authentication** via Google OAuth JWT verification

## Technologies

- **Node.js/Express**: RESTful API framework
- **Redis**: In-memory data store with persistence (RDB/AOF)
- **Elasticsearch 7.x**: Distributed search and analytics engine
- **RabbitMQ**: Message broker implementing AMQP protocol
- **Docker/Docker Compose**: Container orchestration
- **JWT**: Stateless authentication mechanism

## Setup and Installation

### Prerequisites

- Node.js (v14+)
- Docker and Docker Compose

### Installation Steps

#### 1. Clone the repository

```bash
git clone https://github.com/yourusername/healthcare-plan-api.git
cd healthcare-plan-api
```

#### 2.Install dependencies

```bash
npm install
```

#### 3.Start Docker services

```bash
docker-compose up -d
```

#### 4.Run the application

```bash
node app.js
```

The server will be running at http://localhost:3002.

## API Endpoints

### Plan Management

- **Create Plan**: `POST /v1/plan`
- **Get Plan**: `GET /v1/plan/:id`
- **Update Plan**: `PATCH /v1/plan/:id`
- **Delete Plan**: `DELETE /v1/plan/:id`
- **Delete Service from Plan**: `DELETE /v1/plan/:planId/service/:serviceId`

### Search and Query

- **Search Plans**: `GET /v1/search?q=query`
- **Search by Copay**: `GET /v1/plans/by-copay?min=10`

### Elasticsearch Management

- **Check Elasticsearch**: `GET /v1/elasticsearch/check`
- **Search All Documents**: `GET /v1/elasticsearch/search`
- **Get Document Types**: `GET /v1/elasticsearch/document-types`
- **Verify Plan in Elasticsearch**: `GET /v1/plan/:id/verify-elasticsearch`
- **Reindex Plan**: `POST /v1/plan/:id/reindex`

## Authentication

All API endpoints are protected with Google JWT authentication. Include a valid Google JWT token in the request headers:

Authorization: Bearer "your-token"

## Docker Services

The application uses Docker Compose to run multiple services:

- **Elasticsearch Cluster**: A three-node Elasticsearch cluster
- **Kibana**: Web UI for Elasticsearch visualization
- **Redis**: In-memory data store
- **RabbitMQ**: Message queue for asynchronous operations

## Architecture

The application follows a distributed architecture:

1. API requests are authenticated via Google JWT
2. Data is validated against a JSON schema
3. Validated data is stored in Redis for fast access
4. A message is sent to RabbitMQ for asynchronous processing
5. The RabbitMQ consumer indexes the data in Elasticsearch
6. Search queries are directed to Elasticsearch with Redis as fallback

## Note on MongoDB Code

The project initially used MongoDB as its data store. This implementation can be found in the commented code at the end of the `app.js` file. The current implementation uses Redis for better performance and Elasticsearch for advanced search capabilities.

## Troubleshooting

### Common Issues

- **Elasticsearch connection issues**: Check that Elasticsearch containers are running with `docker ps | findstr elastic`
- **Redis connection issues**: Verify Redis is running with `docker exec -it redis redis-cli PING`
- **RabbitMQ connection issues**: Check RabbitMQ status with `docker ps | findstr rabbitmq`

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
