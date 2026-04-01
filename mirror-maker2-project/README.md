# Kafka MirrorMaker 2 Enhancement Project

## Project Overview

This project implements critical enhancements to Apache Kafka MirrorMaker 2 (MM2) to handle edge cases in data replication, particularly focusing on fail-fast mechanisms for log truncation and topic reset scenarios. The improvements ensure data consistency and prevent silent failures during Kafka topic replication between primary and standby clusters.

## Project Structure

```text
kafka/
|
|-- connect/mirror/                     # MirrorMaker 2 source code
|   `-- src/main/java/...
|-- core/                               # Kafka core modules
|-- clients/                            # Kafka clients
|-- build.gradle                        # Kafka build configuration
`-- mirror-maker2-project/              # Validation assets packaged in this branch
      |-- README.md                       # Project documentation
      |-- Test.md                         # Captured test output notes
      |-- docker-compose.yml              # Docker Compose orchestration
      |-- run_challenge.sh                # Test scenario runner script
      |-- config/
      |   `-- mm2.properties              # MirrorMaker 2 configuration
      |-- docker/
      |   `-- kafka/
      |       `-- Dockerfile/
      |           `-- dockerfile          # Dockerfile for custom MM2 image
      `-- Producer/                       # Test producer application
            |-- src/main/java/ProducerApp.java
            |-- build.gradle
            |-- settings.gradle
            |-- gradle.properties
            |-- gradlew
            |-- gradlew.bat
            |-- gradle/
            |   |-- libs.versions.toml
            |   `-- wrapper/
            |       |-- gradle-wrapper.jar
            |       `-- gradle-wrapper.properties
            `-- Dockerfile/
                  `-- dockerfile
```

## 1) Repository Links

- Kafka Fork: Apache Kafka - Main Fork
- Location in this project: `./kafka/`
- Pull Request: MirrorMaker 2 Enhancement PR
- Baseline: Apache Kafka main branch
- Modifications: `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`

## 2) Docker Hub Images

The following container images are used in this project:

| Image | Tag | Purpose | Source |
|---|---|---|---|
| apache/kafka | 4.0.0 | Kafka Primary Cluster | Apache Kafka Official |
| apache/kafka | 4.0.0 | Kafka Standby Cluster | Apache Kafka Official |
| kafka-mirror-maker:latest | latest | Custom MirrorMaker 2 with Enhancements | Built locally from docker/kafka/Dockerfile/dockerfile |
| commit-log-producer | latest | Test Producer Application | Built from Producer/Dockerfile/dockerfile |

### Building Custom Images

```bash
# Build custom MM2 image with enhancements
docker build -t kafka-mirror-maker:latest ./docker/kafka/Dockerfile/

# Build test producer image
docker build -t commit-log-producer ./Producer/Dockerfile/
```

## 3) Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- Approximately 2GB of available memory for containers
- Bash shell for running test scripts

### Initial Setup

Clone the repository and navigate to the project:

```bash
cd KafkaMirrorMakerImprovement
```

Build the enhanced Kafka application:

```bash
cd Kafka/kafka
./gradlew build -x test
cd ../..
```

Build the custom MM2 Docker image:

```bash
docker build -t kafka-mirror-maker:latest ./docker/kafka/Dockerfile/
```

Build the test producer Docker image:

```bash
docker build -t commit-log-producer ./Producer/Dockerfile/
```

Start the containerized environment:

```bash
docker-compose up -d
```

This will:

- Start primary Kafka broker (port 9092)
- Start standby Kafka broker (port 9094)
- Start MirrorMaker 2 connector (`mm2` container)

Verify cluster status:

```bash
# Check if all services are running
docker-compose ps

# Verify primary-kafka is ready
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list"

# Verify standby-kafka is ready
docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list"
```

### Configuration Details

MirrorMaker 2 Configuration (`config/mm2.properties`):

```properties
clusters=primary,standby
primary.bootstrap.servers=primary-kafka:9092
standby.bootstrap.servers=standby-kafka:9092

# Replication direction: primary -> standby only
primary->standby.enabled=true
standby->primary.enabled=false

# Replicate all topics matching pattern
primary->standby.topics=.*

# Replication factors for single-node clusters
replication.factor=1
checkpoints.topic.replication.factor=1
heartbeats.topic.replication.factor=1
offset-syncs.topic.replication.factor=1

# Enable offset and group sync
sync.group.offsets.enabled=true
refresh.topics.enabled=true
refresh.groups.enabled=true
emit.heartbeats.enabled=true
```

## 4) Test Execution

### Running the Test Scenario Suite

Execute the comprehensive test suite that validates all enhancements:

```bash
chmod +x run_challenge.sh
./run_challenge.sh
```

### Test Scenarios

The script `run_challenge.sh` executes three scenarios.

#### Scenario 1: Normal Replication

Purpose: Verify baseline MM2 functionality works correctly.

Steps:

- Produces 20 messages to topic `commit-log` on primary cluster
- Waits for topic to be replicated to standby as `primary.commit-log`
- Consumes messages from standby to verify replication
- Lists all topics on both clusters

Expected Output:

- PASS: Scenario 1 completed
- Topic replicated: `primary.commit-log` visible on standby cluster
- Consumer reads exactly 20 messages

Interpret Results:

- If messages are consumed successfully, the replication pipeline is functioning.

#### Scenario 2: Log Truncation Detection

Purpose: Verify fail-fast behavior when brokers delete log segments.

Steps:

- Sets aggressive retention policy (10 seconds) on `commit-log` topic
- Produces 500 messages, waits for log segments to be deleted
- Produces 500 more messages to force offset mismatch
- Waits 20 seconds for truncation to occur
- Captures MM2 logs and checks for custom truncation detection

Expected Output:

- PASS: Truncation log is printed
- `LOG TRUNCATION DETECTED: topic=commit-log, partition=0, earliestAvailableOffset=XXX. Data loss has occurred.`

Interpret Results:

- PASS: Custom log message confirms fail-fast detection is working
- FAIL: MM2 continued replicating despite log deletion (data inconsistency risk)

#### Scenario 3: Topic Reset Detection and Recovery

Purpose: Verify automatic recovery when topics are deleted and recreated.

Steps:

- Produces 500 initial messages to `commit-log`
- Deletes the topic on primary cluster
- Waits 10 seconds
- Recreates the topic (simulating reset scenario)
- Produces 500 new messages
- Captures MM2 logs and checks for topic reset detection

Expected Output:

- PASS: Reset detection log printed
- `TOPIC RESET DETECTED. Topics affected: [commit-log]. Re-subscribing and seeking to beginning.`
- `Recovery successful. MirrorMaker resumed replication.`

Interpret Results:

- PASS: Custom recovery logs confirm auto-recovery mechanism works
- FAIL: MM2 did not recover automatically (manual intervention required)

### Interpreting Test Results

| Scenario | Pass Criteria | Failure Impact |
|---|---|---|
| 1 | All 20 messages consumed | Replication broken, non-replicated data |
| 2 | `LOG TRUNCATION DETECTED` log found | Silent data loss undetected |
| 3 | `TOPIC RESET DETECTED` log found | Data consistency compromised, manual recovery needed |

## 5) Log Analysis

### Key Log Messages to Monitor

#### 1. Log Truncation Detection (Scenario 2)

```text
[ERROR] LOG TRUNCATION DETECTED: topic=commit-log, partition=0,
      earliestAvailableOffset=12345. Data loss has occurred.
```

- Indicates: Consumer offset became unreachable due to log deletion
- Action Required: Investigate why logs were deleted; verify replication consistency
- Location: MM2 container logs

#### 2. Topic Reset Detection (Scenario 3)

```text
[WARN] TOPIC RESET DETECTED. Topics affected: [commit-log].
     Re-subscribing and seeking to beginning.
[INFO] Recovery successful. MirrorMaker resumed replication.
```

- Indicates: Topic was deleted and recreated; automatic recovery triggered
- Action: Monitor that replication resumes without data gaps
- Location: MM2 container logs

#### 3. Normal Replication Logs

```text
[INFO] Herder started
[INFO] topic: mirror-start-marker, partition: 0
[INFO] Successfully synced group offset
```

- Indicates: MM2 is running normally and replicating topics
- Action: Baseline expected behavior
- Location: MM2 container logs

### Capturing Container Logs

MM2 logs:

```bash
# View recent MM2 logs
docker logs mm2

# Follow MM2 logs in real-time
docker logs -f mm2

# Capture last 10 seconds of logs
docker logs mm2 --since 10s > /tmp/mm2.log
```

Primary Kafka logs:

```bash
docker logs primary-kafka | tail -50
```

Standby Kafka logs:

```bash
docker logs standby-kafka | tail -50
```

Grep for specific events:

```bash
# Search for truncation errors
docker logs mm2 | grep "LOG TRUNCATION DETECTED"

# Search for topic reset
docker logs mm2 | grep "TOPIC RESET DETECTED"

# Search for errors
docker logs mm2 | grep ERROR
```

## 6) Design Rationale

### Problem Statement

Apache Kafka MirrorMaker 2 (MM2) replicates topics between Kafka clusters. However, it was not designed to handle edge cases where:

- Log truncation occurs: Consumer offsets become invalid due to aggressive retention policies
- Topics are reset: Topics are deleted and recreated with new data

In both scenarios, MM2 could silently continue replication with inconsistent data, leading to undetected data loss.

### Solution Architecture

#### 1. Fail-Fast Mechanism for Log Truncation

Implementation: Enhanced `MirrorSourceTask.java` with exception handling.

```java
catch (OffsetOutOfRangeException e) {
   log.error("LOG TRUNCATION DETECTED: topic={}, partition={}, "
      + "earliestAvailableOffset={}. Data loss has occurred.",
      topicPartition.topic(), topicPartition.partition(), earliestOffset);
   throw new KafkaException("Fail-fast due to log truncation", e);
}
```

Why this approach:

- Immediately stops replication when data loss is detected
- Prevents replication of inconsistent state
- Forces operator to investigate and handle the issue
- Provides forensic information (earliest available offset) for debugging

Data flow:

```text
Primary Cluster -> [Deleted Logs] -> Consumer seeks failed
                      |
                      v
              OffsetOutOfRangeException caught
                      |
                      v
              Custom ERROR logged
                      |
                      v
              Replication STOPS
```

#### 2. Automatic Topic Reset Recovery

Implementation: Exception handling for `UnknownTopicOrPartitionException`.

```java
catch (UnknownTopicOrPartitionException e) {
   log.warn("TOPIC RESET DETECTED. Topics affected: {}. "
      + "Re-subscribing and seeking to beginning.", topics);

   consumer.unsubscribe();
   consumer.subscribe(topics);
   consumer.seekToBeginning(newAssignments);
   log.info("Recovery successful. MirrorMaker resumed replication.");
}
```

Why this approach:

- Automatically recovers when topic is deleted and recreated
- No manual intervention required
- Resumes replication from the beginning of the new topic
- Logs all recovery actions for audit trail

Data flow:

```text
Topic Deleted -> [Recreation] -> Topic exists again
                    |
                    v
              UnknownTopicOrPartitionException caught
                    |
                    v
              Consumer unsubscribes
                    |
                    v
              Consumer re-subscribes to new partition
                    |
                    v
              Consumer seeks to beginning
                    |
                    v
              Replication RESUMES with new data
```

### Integration Points

Modified files:

- `Kafka/kafka/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`
- Line: `poll()` method
- Changes: Added try-catch blocks for exception handling
- Impact: Every message fetch operation is now protected

No breaking changes:

- All existing MM2 functionality preserved
- Enhanced logging only adds to log output
- Exception handling is additive (wraps existing logic)
- Compatible with existing MM2 configurations

### Performance Impact

- Minimal: Exception handling only activates on error conditions
- Normal operations: No performance degradation
- Edge cases: Immediate failure detection improves resource efficiency by stopping replication of bad data

### Monitoring and Alerting Strategy

Set alerts for these log patterns:

- `ERROR: LOG TRUNCATION DETECTED` -> Critical: Immediate investigation
- `WARN: TOPIC RESET DETECTED` -> Warning: Review recovery actions
- `ERROR: KafkaException` -> Check if caused by truncation

## 7) AI Usage Documentation

### Tools and Technologies Used

- GitHub Copilot: Used for code generation, error analysis, and understanding error patterns
- ChatGPT: Used for conceptual understanding, architectural design, and documentation

### Methodology

1. Project setup and debugging
  AI tools were used to understand and troubleshoot errors during initial project setup, Kafka build process, and test simulation. This accelerated debugging when encountering configuration and environment-specific issues.
2. Kafka architecture understanding
  AI assistance helped explain Apache Kafka source internals, especially how the primary cluster, MirrorMaker 2, and standby cluster interact through the codebase.
3. Design decisions
  AI tools supported decisions around project structure, JAR selection in Docker images, replication configuration strategy, and test design.
4. Infrastructure and testing
  AI assisted with designing `run_challenge.sh`, `mm2.properties`, Docker Compose orchestration, and Dockerfiles.
5. Failure-handling implementation
  AI guidance helped identify suitable exception types (`OffsetOutOfRangeException`, `UnknownTopicOrPartitionException`) and robust recovery patterns in `poll()`.

### Key Contributions from AI

- Accelerated learning curve for Apache Kafka internal architecture
- Provided code patterns for exception handling and recovery
- Suggested optimization strategies for normal and edge-case scenarios
- Helped design comprehensive test scenarios
- Assisted with documentation of architectural concepts
- Improved code quality through better error-handling suggestions

## Cleanup

Clean up Docker resources:

```bash
# Stop and remove all containers
docker-compose down

# Remove all dangling containers
docker container prune -f

# Remove images
docker rmi kafka-mirror-maker:latest commit-log-producer

# Clear KRaft logs (if using local volumes)
rm -rf /tmp/kraft-combined-logs
```

## run_challenge.sh script output.

```text
=== 🚀 MM2 Test Script (On-demand Producer) ===

🌐 [EVENT] Using Docker network: mirrormaker2_default

⏳ [EVENT] Waiting for primary Kafka...
✅ [EVENT] Primary Kafka is ready

⏳ [EVENT] Waiting for standby Kafka...
✅ [EVENT] Standby Kafka is ready

⏳ [EVENT] Waiting for MirrorMaker2...
✅ [EVENT] MirrorMaker2 is running

📌 [EVENT] Creating topic: commit-log
⏳ [EVENT] Waiting for MM2 topic discovery...

===============================
✅ Scenario 1: Normal Replication
================================

📤 [EVENT] Producing 20 messages...

⏳ [EVENT] Waiting for topic replication in standby...
📥 [EVENT] Consuming from standby...

🔍 [EVENT] Records replicated: 20
✅ TEST 1 PASSED

===============================
🔥 Scenario 2: Log Truncation
=============================

⏸️ [EVENT] Pausing MM2...

⚙️ [EVENT] Setting aggressive retention (60 sec)...
📤 [EVENT] Producing 500 messages...
📤 [EVENT] Producing 500 messages...

⏳ [EVENT] Waiting for truncation...

▶️ [EVENT] Resuming MM2...

📡 [EVENT] Capturing MM2 logs...

📄 [EVENT] Checking truncation detection...
❌ TEST 2 FAILED

===============================
🔥 Scenario 3: Topic Reset
==========================

⏸️ [EVENT] Pausing MM2...

📤 [EVENT] Producing 500 messages...

🗑️ [EVENT] Deleting topic...
♻️ [EVENT] Recreating topic...

▶️ [EVENT] Resuming MM2...

📤 [EVENT] Producing 500 messages...

📡 [EVENT] Capturing MM2 logs...

📄 [EVENT] Checking topic reset detection...
✅ TEST 3 PASSED

📄 [EVENT] Checking recovery...
✅ RECOVERY PASSED

📄 [MM2 LOG]
[MM2-FIX][TOPIC-RESET] Topic commit-log recreated. Reset detected.
[MM2-FIX][RECOVERY] Reinitializing offsets and resuming replication from earliest.
[MM2-FIX][RECOVERY] Recovery successful. Replication resumed without data loss.

🎯 ALL TESTS COMPLETED
```
