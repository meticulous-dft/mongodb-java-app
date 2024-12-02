# MongoDB Java Sample Application

This project is designed to test MongoDB cluster scaling by simulating heavy read and write
workloads. It consists of two phases:

1. Data Loading: Loads a specified amount of data into MongoDB
2. Load Testing: Performs read and write operations according to specified ratios

## Prerequisites

- Java 17 or higher
- Maven (for building)
- Docker (optional)
- MongoDB Atlas cluster or other MongoDB deployment

## Environment Variables

The application requires the following environment variables:

```bash
MONGODB_URI           # MongoDB connection string
MONGODB_DATABASE      # Database name
MONGODB_COLLECTION    # Collection name
TOTAL_DATA_SIZE_GB    # Total data size to load in GB
WRITE_PERCENTAGE      # Percentage of write operations during load test (0-100)
NUM_THREADS           # Number of concurrent threads
TARGET_DOCUMENT_SIZE  # Target size of each document in bytes (default: 4096)
SHARDED            # Whether to enable sharding (default: false)
LOG_LEVEL             # Log level (default: INFO)
MONGODB_LOG_LEVEL     # MongoDB driver log level (default: INFO)
```

## Project Structure

```
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── example/
│       │           ├── Main.java
│       │           ├── Config.java
│       │           └── DocumentGenerator.java
│       └── resources/
│           └── logback.xml
├── Dockerfile
├── pom.xml
└── README.md
```

## Building Locally

```bash
bazel build //:mongodb-java-app_deploy.jar
```

## Running Locally

1. Data Loading Phase:

```bash
java -jar bazel-bin/mongodb-java-app_deploy.jar load
```

2. Load Testing Phase:

```bash
java -jar bazel-bin/mongodb-java-app_deploy.jar
```

3. Stress Testing:

   Stress test run cpu intensive operations using all available threads, and then has a separate
   thread to do operations on the database.

```bash
java -jar bazel-bin/mongodb-java-app_deploy.jar stress
```

## Docker Build

```bash
docker build -t mongodb-java-app .
```

## Running with Docker

1. Data Loading Phase:

```bash
docker run --rm -e MONGODB_URI=<uri> -e MONGODB_DATABASE=<db> -e MONGODB_COLLECTION=<collection> \
    -e TOTAL_DATA_SIZE_GB=<size> -e WRITE_PERCENTAGE=<percentage> -e NUM_THREADS=<threads> \
    mongodb-java-app load
```

2. Load Testing Phase:

```bash
docker run --rm -e MONGODB_URI=<uri> -e MONGODB_DATABASE=<db> -e MONGODB_COLLECTION=<collection> \
    -e TOTAL_DATA_SIZE_GB=<size> -e WRITE_PERCENTAGE=<percentage> -e NUM_THREADS=<threads> \
    mongodb-java-app test
```

## Monitoring

The application provides metrics for:

- Total operations performed
- Read/Write operation counts
- Failed operations
- Operation latencies
