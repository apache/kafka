# Apache Kafka MirrorMaker 2 Hardening Project

## 1. Overview
This project focuses on enhancing the fault tolerance, resiliency, and data integrity of Apache Kafka's **MirrorMaker 2 (MM2)** within a mission-critical multi-cluster cross-regional data replication environment. 

In enterprise deployments, topics function as Write-Ahead Logs (WAL) containing immutable sequential state changes. While vanilla MirrorMaker 2 natively handles basic transport disruptions and intermittent network loss, it fails to handle or alert on complex data anomalies. Specifically, if upstream data segments are dropped (log truncation) or an upstream topic is recreated (topic reset), vanilla MirrorMaker 2 can silently duplicate data gaps or drop messages, resulting in state drift on the disaster recovery (DR) cluster. 

This repository hardens the replication core to enforce dynamic state validation, executing explicit **Fail-Fast crashes** upon detecting un-replicated data truncation gaps and implementing **graceful, automated recovery loops** after catastrophic topic resets.

---

### 1.1 Project Repository & Core Codebase Links

The complete source codebase has been successfully published to a personal tracking repository. To evaluate the architectural changes and hardening enhancements without wading through framework boilerplate, please use the direct file tracking links below:

* **Hardened Kafka Repository Home:** [https://github.com/rajabhishekmaurya/kafka](https://github.com/rajabhishekmaurya/kafka)
* **Custom MirrorMaker 2 Logic:** [MirrorSourceTask.java Core Implementation](https://github.com/rajabhishekmaurya/kafka/blob/main/kafka-fork/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java)
* **Automated Verification Harness:** [run_challenge.sh Test Suite Script](https://github.com/rajabhishekmaurya/kafka/blob/main/run_challenge.sh)
* **Cluster Deployment Layout:** [docker-compose.yml Infrastructure Specification](https://github.com/rajabhishekmaurya/kafka/blob/main/docker-compose.yml)
  
  
## 2. Design Rationale & MM2 Enhancements

The architectural modifications are entirely encapsulated within `MirrorSourceTask.java`. They intercept and manage severe offset alignment issues before data gaps propagate down the stream.

### Task 2: Fail-Fast Truncation Detector
When upstream records are purged due to strict retention or log compaction rules before they can be replicated, a data gap is created. Vanilla MirrorMaker 2's underlying consumer will self-heal by silently shifting forward via its internal `auto.offset.reset=earliest` directive. This leaves the target cluster permanently missing a segment of historical truth.
* **Our Modification:** Inside the active execution `poll()` loop, before extracting messages, the code queries the source broker for the lowest available log watermarks (`beginningOffsets`).
* **Enforced Mechanic:** If the current tracking position (`consumer.position()`) is found to be *strictly less than* the source broker's minimum available offset, it indicates that a data gap has been permanently dropped from the primary cluster. 
* **Outcome:** The engine bypasses silent self-healing, prints a distinct `Source log truncation detected` error message via SLF4J, and raises a critical `KafkaException` to immediately crash the thread container. This alerts operational monitoring tools rather than allowing silent data loss.

### Task 3: Graceful Topic Reset Handling
When an administrative team drops and recreates an upstream topic on the primary cluster, the topic's unique identifier (UUID) and epoch structures drop back to zero. A running consumer will throw exceptions or hang due to stale tracking metadata.
* **Our Modification:** The code monitors the broker state boundary transitions. It intercepts conditions where the primary cluster's `beginningOffset` is reset to `0`, while the internal offset reader holds a high legacy position index.
* **Enforced Mechanic:** Upon identifying this mismatch (either during initialization inside `initializeConsumer()` or during runtime validation blocks), the task catches the reset. It re-aligns the consumer tracking pointers back to `0L` via `consumer.seek()`.
* **Outcome:** MirrorMaker 2 automatically recovers from a severe infrastructure rebuild, smoothly streaming post-reset operational events to the DR cluster with zero manual intervention or service redeployment.

---

## 3. Setup Instructions

The topology is built using containerized environments utilizing official **Apache Kafka 4.0.0** architectures operating in KRaft mode.

### Prerequisites
* Linux or macOS environment
* Docker Engine and Docker Compose (v3.9+)
* Java Development Kit (JDK 17+) & a build tool (Gradle/Maven) to compile source changes

### Core Infrastructure Components (`docker-compose.yml`)
* **`primary`**: The production authoritative broker cluster running KRaft mode on internal port `9092`.
* **`standby`**: The isolated Disaster Recovery (DR) cluster running KRaft mode on internal port `9094`.
* **`mirror-maker`**: The cross-cluster replication engine that embeds our hardened `.jar` files to sync data from `primary` to `standby`.
* **`producer`**: A synthetic workload generator that produces continuous JSON events to simulate system state changes.

---

## 4. Test Execution & Scenario Verification

A unified test harness (`run_challenge.sh`) validates all three architectural requirements sequentially.

### Step 1: Recompile and Build Codebase Modifications
Whenever you alter validation patterns inside `MirrorSourceTask.java`, compile your project to refresh your destination `.jar` files, and instruct Docker to bypass its layer cache to ensure the fresh bytecode is injected:

```bash
# Compile source files locally to produce the updated jar libraries
# Inside ~/..path-to-/java-kafka$
cd kafka-fork
./gradlew jar
cd ..

# Build the MirrorMaker service container by forcing a complete cache bypass
docker-compose build --no-cache mirror-maker
```

### Step 2: Execute the Test Suite
Trigger the test harness to run all verification workflows automatically:

```bash
# Clear lingering persistent volume states and launch the test script
# Inside ~/..path-to-/java-kafka$
docker-compose down -v && ./run_challenge.sh
```


## Script Scenarios & Expected Behavior

---

### SCENARIO 1: Normal Replication Flow

The script spins up the primary and standby nodes, registers the production `commit-log` topic, and initiates the pipeline. The producer streams **1,000 JSON messages**.

**Verification Method**

Rather than relying on fragile terminal string matchers, the script audits the write-ahead log directly on the standby broker's file system using disk allocation boundaries (`du -b`).

**Expected Output**

The standby cluster logs storage expands to **over 35,000 bytes**, indicating data is moving correctly across the clusters.

**Terminal Confirmation**

```
SUCCESS: Normal replication verified. Standby cluster storage populated successfully.
```

---

### SCENARIO 2: Log Truncation Simulation (Task 2 Fail-Fast)

The test runner pauses the MirrorMaker container to stack up un-replicated records on the primary node. It calls Kafka's administrative utility (`kafka-delete-records.sh`) to force an artificial log truncation gap by shifting the low watermark up to `1050`. It then unpauses MirrorMaker.

**Verification Method**

The updated logic discovers that its expected offset tracker is lower than the active low watermark on the broker. It bypasses self-healing, prints the error trace, and executes an intentional panic crash.

**Expected Output**

The test harness monitors the container crash dumps, finds the custom error footprint, and terminates the container gracefully.

**Terminal Confirmation**

```
SUCCESS: Task 2 Verified! MirrorMaker caught the data loss anomaly and executed a Fail-Fast crash.
```

---

### SCENARIO 3: Topic Reset Simulation (Task 3 Recovery)

The script restarts the MirrorMaker container using `docker-compose restart` to clear the intentional crash state from Scenario 2. It simulates a major maintenance incident by deleting and completely recreating the primary `commit-log` topic. It then produces **100 new post-reset events**.

**Verification Method**

The replication engine intercepts the zeroed beginning watermark, safely resets its internal tracking positions to `0L`, and resumes replication.

**Expected Output**

The disk storage on the standby broker expands significantly past its historical volume baseline, proving post-reset records are syncing correctly.

**Terminal Confirmation**

```
SUCCESS: Task 3 Verified! MirrorMaker gracefully recovered and resumed replicating new records.
```

---

## 5. Log Analysis Guidance

To continuously audit or evaluate data integrity across these nodes, monitor the container logs for these critical SLF4J signatures.

---

**Verifying a Truncation Deficit Crash**

When an administrative prune or hardware data drop moves the beginning offset past where MirrorMaker expects to consume, your application log output will print:

```
[ERROR] org.apache.kafka.connect.mirror.MirrorSourceTask - Source log truncation detected for partition commit-log-0! Current replication position X is behind source log start offset Y.
org.apache.kafka.common.KafkaException: Source log truncation detected
    at org.apache.kafka.connect.mirror.MirrorSourceTask.poll(MirrorSourceTask.java)
```

This logs the structural event gap and terminates the connector thread immediately to prevent downstream corruption.

---

**Verifying a Topic Reset Event**

During an administrative deletion and creation loop, keep an eye out for initial metadata warnings followed by a successful partition reassignment sequence:

```
[WARN] org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=mm2-source-consumer] Synchronous auto-commit of offsets failed: Commit cannot be completed since the group has already rebalanced.
[INFO] org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListener - Partitions revoked / assigned dynamically... Resuming stream replication tracking at offset 0.
```

---

## 5. Pre-Submission Cleanup

Before archiving or submitting the project repository, it is highly recommended to completely tear down the local container ecosystem and wipe any temporary build targets. This ensures no heavy virtual log files, transient brokers states, or target binary artifacts are packaged into the deliverable.

Run the following commands in the project root directory:

```bash
# 1. Tear down the cluster infrastructure and delete heavy virtual storage volumes
docker-compose down -v

# 2. Clean out temporary Java compilation build directories
./gradlew clean   # If your environment uses Gradle
# mvn clean       # If your environment uses Maven
```

## 6. Code Quality & Verification Matrix

| Evaluation Test Case | Target Metric Verified                | Engineering Mechanism                                   | Status |
| -------------------- | ------------------------------------- | ------------------------------------------------------- | ------ |
| Scenario 1           | Cross-Cluster Replication Baseline    | Disk volume byte validation (`du -b`)                   | PASSED |
| Scenario 2           | Fail-Fast Crash on Anomaly Data Gap   | Administrative truncation via `kafka-delete-records.sh` | PASSED |
| Scenario 3           | Zero-Downtime Automated Recovery Loop | Topic Deletion and Recreation sequence                  | PASSED |

---
