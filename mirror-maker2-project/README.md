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
docker build -t enhanced-mirror-maker2:latest ./docker/kafka/Dockerfile/

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
cd kafka
./gradlew connect:mirror:build -x test
cd ../..
```

Build the custom MM2 Docker image:

```bash
docker build -t enhanced-mirror-maker2:latest ./docker/kafka/Dockerfile/
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
Do docker-compose up --build in sepearte terminal and 
./run_challenge.sh in separate terminal
The script `run_challenge.sh` executes three scenarios.

#### Scenario 1: Normal Replication

Purpose: Verify baseline MM2 functionality works correctly.

Steps:

- Produces 20 messages to topic `commit-log` on primary cluster
- Waits for topic to be replicated to standby as `primary.commit-log`
- Consumes messages from standby to verify replication
- Lists all topics on both clusters

Interpret Results:

- If messages are consumed successfully, the replication pipeline is functioning.
- This test validates that normal replication without edge cases works as expected.

#### Scenario 2: Log Truncation Detection

Purpose: Verify fail-fast behavior when brokers delete log segments.

Steps:

- Pauses MM2 to prevent offset consumption
- Sets aggressive retention policy (60 seconds) on `commit-log` topic
- Produces 500 messages, waits for log segments to be eligible for deletion
- Produces 500 more messages to force offset mismatch
- Resumes MM2 and waits for truncation detection
- Captures MM2 logs and checks for custom truncation detection

Expected Output:

- PASS: Truncation log is printed with offset regression detected
- `🔥[OFFSET REGRESSION DETECTED] topic-partition=..., lastOffset=..., currentOffset=...`
- `🔥[TRUNCATION DETECTED] ...`


Interpret Results:

- PASS: Custom log message confirms fail-fast detection is working
- FAIL: MM2 continued replicating despite log deletion (data inconsistency risk)
- Note: This scenario requires fine-tuning of retention policies and timing

#### Scenario 3: Topic Reset Detection and Recovery

Purpose: Verify automatic recovery when topics are deleted and recreated.

Steps:

- Pauses MM2 to prevent offset consumption
- Produces 500 initial messages to `commit-log`
- Deletes the topic on primary cluster
- Waits 10 seconds for topic deletion to propagate
- Recreates the topic (simulating reset scenario)
- Resumes MM2 and produces 500 new messages
- Captures MM2 logs and checks for topic reset detection and recovery

Expected Output:

- PASS: Reset detection and recovery logs printed
- `[MM2-FIX][TOPIC-RESET] Topic commit-log recreated. Reset detected.`
- `[MM2-FIX][RECOVERY] Reinitializing offsets and resuming replication from earliest.`
- `[MM2-FIX][RECOVERY] Recovery successful. Replication resumed without data loss.`




## 5) Log Analysis

### Key Log Messages to Monitor

#### 1. Offset Regression Detection (Scenario 2 - Log Truncation)

```text
🔥[OFFSET REGRESSION DETECTED] topic-partition=commit-log-0, 
lastOffset=100, currentOffset=95
🔥[TRUNCATION DETECTED] commit-log-0
```

- Indicates: Consumer offset became unreachable due to log deletion/compaction
- Pattern: currentOffset < lastOffset (backward movement)
- Action Required: Investigate why logs were deleted; verify replication consistency
- Location: MM2 container logs, MirrorSourceTask.detectTruncation() output

#### 2. Topic Reset Detection & Automatic Recovery (Scenario 3)

```text
🔥[OFFSET REGRESSION DETECTED] topic-partition=commit-log-0,
lastOffset=500, currentOffset=0
🔥[TOPIC RESET DETECTED] commit-log-0
🔥[TOPIC RESET RECOVERY] Starting recovery...
🔥 Seeking to beginning for [commit-log-0]
🔥[RECOVERY SUCCESS] Topic reset handled
```

Or with MM2 logs output:

```text
[MM2-FIX][TOPIC-RESET] Topic commit-log recreated. Reset detected.
[MM2-FIX][RECOVERY] Reinitializing offsets and resuming replication from earliest.
[MM2-FIX][RECOVERY] Recovery successful. Replication resumed without data loss.
```

- Indicates: Topic was deleted and recreated; automatic recovery triggered
- Pattern: currentOffset==0 after offset regression (currentOffset < lastOffset)
- Action: Monitor that replication resumes without data gaps
- Recovery: Automatic - consumer seeks to beginning and continues polling
- Location: MM2 container logs, MirrorSourceTask.detectTruncation() and handleTopicReset() output

#### 3. Offset Jump Forward (Scenario 1 - Normal with gap detection)

```text
⚠️ Offset jump detected (possible data loss) tp=commit-log-0,
last=100, current=105
```

- Indicates: Consumer skipped offsets (possible data loss or filtered messages)
- Pattern: currentOffset > (lastOffset + 1)
- Action: Investigate source of missing offsets; check for message filtering transforms
- Severity: Warning-level - may indicate legitimate filtering
- Location: MM2 container logs, MirrorSourceTask.detectTruncation() output

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

#### 1. Offset Regression Detection & Fail-Fast for Log Truncation

Implementation: Enhanced `MirrorSourceTask.java` with `detectTruncation()` method tracking offset state per partition.

```java
private final Map<TopicPartition, Long> lastSeenOffsets = new HashMap<>();

private void detectTruncation(TopicPartition tp, long currentOffset) {
    Long lastOffset = lastSeenOffsets.get(tp);
    
    if (lastOffset == null) {
        return;  // First offset, no baseline yet
    }
    
    // ✅ CASE 1: NORMAL FLOW
    if (currentOffset == lastOffset + 1) {
        return;  // Expected sequential offset
    }
    
    // 🔥 CASE 2: OFFSET REGRESSION (real problem detected)
    if (currentOffset <= lastOffset) {
        log.error("🔥[OFFSET REGRESSION DETECTED] topic-partition={}, "
            + "lastOffset={}, currentOffset={}", tp, lastOffset, currentOffset);
        
        if (currentOffset == 0) {
            // Topic was reset - attempt recovery
            handleTopicReset();
            return;
        }
        
        // Unexpected backward jump - truncation
        throw new KafkaException("Log truncation detected for " + tp +
            " last=" + lastOffset + " current=" + currentOffset);
    }
    
    // 🔥 CASE 3: OFFSET JUMP FORWARD (possible data loss)
    if (currentOffset > lastOffset + 1) {
        log.warn("⚠️ Offset jump detected (possible data loss) tp={}, "
            + "last={}, current={}", tp, lastOffset, currentOffset);
    }
}
```

Why this approach:

- **Real-time Detection**: Monitors offset progression per partition during poll()
- **Three-case Logic**: Distinguishes normal flow, resets, and truncation
- **Fail-Fast on Truncation**: Throws exception when backward offset jump detected
- **Auto-Recovery on Reset**: Catches offset==0 pattern and triggers recovery
- **Forensic Data**: Tracks last seen offset for debugging

Data flow:

```text
Primary Cluster -> ConsumerRecord (offset=N)
                      |
                      v
         detectTruncation(offset=N) called
                      |
         +---------------------+---------------------+
         |                     |                     |
      N = L+1              N < L              N > L+1
    (normal)           (regression)          (jump)
         |                     |                     |
      return            isReset? (N==0)         warn
                    |
                 YES|        NO
                    |         |
              handleReset   throw
```

#### 2. Automatic Topic Reset Recovery

Implementation: `handleTopicReset()` method triggered when offset regression with offset==0 is detected.

```java
private void handleTopicReset() {
    log.error("🔥[TOPIC RESET RECOVERY] Starting recovery...");
    
    try {
        Set<TopicPartition> partitions = new HashSet<>(consumer.assignment());
        
        if (partitions.isEmpty()) {
            log.warn("No partitions assigned during reset recovery.");
            return;
        }
        
        log.error("🔥 Seeking to beginning for {}", partitions);
        consumer.seekToBeginning(partitions);
        
        // ✅ CRITICAL → clear offset state
        lastSeenOffsets.clear();
        
        log.error("🔥[RECOVERY SUCCESS] Topic reset handled");
        
    } catch (Exception ex) {
        log.error("🔥[RECOVERY FAILED]", ex);
        throw new KafkaException("Topic reset recovery failed", ex);
    }
}
```

Why this approach:

- **Triggered by Reset Pattern**: Detects when offset==0 after regression
- **State Cleanup**: Clears lastSeenOffsets map to reset tracking
- **Automatic Recovery**: No manual intervention required
- **Consumer Repositioning**: Seeks to beginning of recreated topic
- **Audit Trail**: Logs all recovery steps including success/failure
- **Fail-Safe**: If recovery fails, throws exception to alert operators

Data flow:

```text
Topic Deleted -> [Recreated with new logs] -> Consumer gets offset==0
                              |
                              v
                   Offset regression detected (offset < lastOffset)
                              |
                              v
                      Is offset == 0? YES
                              |
                              v
                     handleTopicReset() called
                              |
                              v
                 Consumer seeks to beginning of new topic
                              |
                              v
                  Clear lastSeenOffsets tracking state
                              |
                              v
                    Replication RESUMES with new data
                              |
                        (no data loss)
```

### Integration Points

Modified files:

- `kafka/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`
- Location: `poll()` method (called for every message fetch cycle)
- Changes Added:
  - Field: `Map<TopicPartition, Long> lastSeenOffsets` - tracks offset per partition
  - Method: `detectTruncation(TopicPartition tp, long currentOffset)` - validates offset progression
  - Method: `handleTopicReset()` - automatic recovery from topic recreation
  - Hook: Called in poll() for each ConsumerRecord before processing
- Impact: Every message fetch operation is now protected with offset regression detection

Modification Pattern:

```java
public List<SourceRecord> poll() {
    // ... existing poll setup ...
    for (ConsumerRecord<byte[], byte[]> record : records) {
        // NEW: Detect offset regression/truncation/reset
        TopicPartition sourcePartition = 
            new TopicPartition(record.topic(), record.partition());
        detectTruncation(sourcePartition, record.offset());
        lastSeenOffsets.put(sourcePartition, record.offset());
        
        // ... existing conversion and processing ...
    }
    // ... existing error handling ...
}
```

No breaking changes:

- All existing MM2 functionality preserved
- Enhanced logging only adds to log output
- Exception handling is additive (wraps existing logic)
- Compatible with existing MM2 configurations
- Backward compatible with standard Kafka clients

### Performance Impact

- Minimal: Exception handling only activates on error conditions
- Normal operations: No performance degradation
- Edge cases: Immediate failure detection improves resource efficiency by stopping replication of bad data

### Monitoring and Alerting Strategy

Set alerts for these log patterns:

**Critical Alerts (requires immediate action):**
- `🔥[OFFSET REGRESSION DETECTED]` -> Offset backward movement detected
- `🔥[TRUNCATION DETECTED]` -> Log truncation confirmed, replication stopped
- `🔥[RECOVERY FAILED]` -> Topic reset recovery failed, manual intervention needed

**Warning Alerts (review but may auto-recover):**
- `🔥[TOPIC RESET DETECTED]` -> Topic recreated, attempting auto-recovery
- `🔥[TOPIC RESET RECOVERY]` -> Recovery in progress
- `⚠️ Offset jump detected` -> Possible data loss or message filtering

**Informational Logs (expected during recovery):**
- `🔥[RECOVERY SUCCESS]` -> Topic reset recovery completed successfully
- `🔥 Seeking to beginning` -> Consumer repositioning to new topic start

**Search commands for log analysis:**

```bash
# Find all offset regressions
docker logs mm2 | grep "OFFSET REGRESSION"

# Find truncation detections (failures)
docker logs mm2 | grep "TRUNCATION DETECTED"

# Find topic reset events
docker logs mm2 | grep "TOPIC RESET"

# Find recovery attempts
docker logs mm2 | grep "RECOVERY"

# Find offset jumps (warnings)
docker logs mm2 | grep "Offset jump detected"
```

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
## Output of the test script and unit test cases for connect:mirror module.
## run_challenge.sh script output.

```text
@nitinbs1999 ➜ /workspaces/codespaces-blank/MirrorMaker2 $ ./run_challenge.sh 
=== 🚀 MM2 Test Script (Enhanced Validation) ===
🌐 Using Docker network: mirrormaker2_default
⏳ Waiting for primary Kafka...
✅ Primary Kafka ready
⏳ Waiting for standby Kafka...
✅ Standby Kafka ready
⏳ Waiting for MM2...
✅ MM2 running
📌 Creating topic: commit-log

===============================
✅ Scenario 1: Normal Replication
===============================
📤 Producing 1000 messages...
🔍 Replicated records: 1000
✅ RESULT: TEST 1 PASSED

===============================
🔥 Scenario 2: Log Truncation
===============================
⏸️ [EVENT] Pausing MM2...
mm2
⚙️ Applying retention (5s)...
📤 Producing 1000 messages...
📤 Producing 1000 messages...
⏳ Waiting for truncation...
▶️ [EVENT] Resuming MM2...
mm2

📡 [EVENT] Capturing MM2 logs (last 20s)...
🔎 Truncation log detected
🔎 Fail-fast log detected
✅ RESULT: TEST 2 PASSED


===============================
🔥 Scenario 3: Topic Reset
===============================
⏸️ [EVENT] Pausing MM2...
mm2
📤 Producing 300 messages...
🗑️ Deleting topic...
♻️ Recreating topic...
▶️ [EVENT] Resuming MM2...
mm2
📤 Producing 300 messages...

📡 [EVENT] Capturing MM2 logs (last 20s)...
🔎 Reset log detected
🔍 Records after reset: 2445292
✅ RESULT: TEST 3 PASSED

🎯 ALL TESTS COMPLETED
```
