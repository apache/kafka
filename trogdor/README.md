Trogdor
========================================
Trogdor is a test framework for Apache Kafka.

Trogdor can run benchmarks and other workloads. Trogdor can also inject faults in order to stress test the system.

Quickstart
=========================================================
First, we want to [start a single-node Kafka cluster in KRaft mode](https://github.com/apache/kafka/blob/trunk/README.md#running-a-kafka-broker-in-kraft-mode)

Running Kafka in Kraft mode:

```
KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
./bin/kafka-server-start.sh config/kraft/server.properties  &> /tmp/kafka.log &
```
Then, we want to run a Trogdor Agent, plus a Trogdor Coordinator.

To run the Trogdor Agent:

```
./bin/trogdor.sh agent -c ./config/trogdor.conf -n node0 &> /tmp/trogdor-agent.log &
```
To run the Trogdor Coordinator:

```
./bin/trogdor.sh coordinator -c ./config/trogdor.conf -n node0 &> /tmp/trogdor-coordinator.log &
```
Let's confirm that all of the daemons are running:

```
jps
116212 Coordinator
115188 QuorumPeerMain
116571 Jps
115420 Kafka
115694 Agent
```
Now, we can submit a test job to Trogdor.

```
./bin/trogdor.sh client createTask -t localhost:8889 -i produce0 --spec ./tests/spec/simple_produce_bench.json
Sent CreateTaskRequest for task produce0.
```
We can run showTask to see what the task's status is:

```
./bin/trogdor.sh client showTask -t localhost:8889 -i produce0
Task bar of type org.apache.kafka.trogdor.workload.ProduceBenchSpec is DONE. FINISHED at 2019-01-09T20:38:22.039-08:00 after 6s
```
To see the results, we use showTask with --show-status:

```
./bin/trogdor.sh client showTask -t localhost:8889 -i produce0 --show-status
Task bar of type org.apache.kafka.trogdor.workload.ProduceBenchSpec is DONE. FINISHED at 2019-01-09T20:38:22.039-08:00 after 6s
Status: {
  "totalSent" : 50000,
  "averageLatencyMs" : 17.83388,
  "p50LatencyMs" : 12,
  "p95LatencyMs" : 75,
  "p99LatencyMs" : 96,
  "transactionsCommitted" : 0
}
```
Trogdor Architecture
========================================
Trogdor has a single coordinator process which manages multiple agent processes.  Each agent process is responsible for a single cluster node.

The Trogdor coordinator manages tasks.  A task is anything we might want to do on a cluster, such as running a benchmark, injecting a fault, or running a workload.  In order to implement each task, the coordinator creates workers on one or more agent nodes.

The Trogdor agent process implements the tasks.  For example, when running a workload, the agent process is the process which produces and consumes messages.

Both the coordinator and the agent expose a REST interface that accepts objects serialized via JSON.  There is also a command-line program which makes it easy to send messages to either one without manually crafting the JSON message body.

All Trogdor RPCs are idempotent except the shutdown requests.  Sending an idempotent RPC twice in a row has the same effect as sending the RPC once.

Tasks
========================================
Tasks are described by specifications containing:

* A "class" field describing the task type.  This contains a full Java class name.
* A "startMs" field describing when the task should start.  This is given in terms of milliseconds since the UNIX epoch.
* A "durationMs" field describing how long the task should last.  This is given in terms of milliseconds.
* Other fields which are task-specific.

The task specification is usually written as JSON.  For example, this task specification describes a network partition between nodes 1 and 2, and 3:

    {
        "class": "org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec",
        "startMs": 1000,
        "durationMs": 30000,
        "partitions": [["node1", "node2"], ["node3"]]
    }
    
This task runs a simple ProduceBench test on a cluster with one producer node, 5 topics, and 10,000 messages per second. 
The keys are generated sequentially and the configured partitioner (DefaultPartitioner) is used. 

    {
        "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
        "durationMs": 10000000,
        "producerNode": "node0",
        "bootstrapServers": "localhost:9092",
        "targetMessagesPerSec": 10000,
        "maxMessages": 50000,
        "activeTopics": {
            "foo[1-3]": {
                "numPartitions": 10,
                "replicationFactor": 1
            }
        },
        "inactiveTopics": {
             "foo[4-5]": {
                 "numPartitions": 10,
                 "replicationFactor": 1
             }
        },
        "keyGenerator": {
             "type": "sequential",
             "size": 8,
             "offset": 1
        },
        "useConfiguredPartitioner": true
     }

Tasks are submitted to the coordinator.  Once the coordinator determines that it is time for the task to start, it creates workers on agent processes.  The workers run until the task is done.

Task specifications are immutable; they do not change after the task has been created.

Tasks can be in several states:
* PENDING, when task is waiting to execute,
* RUNNING, when the task is running,
* STOPPING, when the task is in the process of stopping,
* DONE, when the task is done.

Tasks that are DONE also have an error field which will be set if the task failed.

Workloads
========================================
Trogdor can run several workloads.  Workloads perform operations on the cluster and measure their performance.  Workloads fail when the operations cannot be performed.

### ProduceBench
ProduceBench starts a Kafka producer on a single agent node, producing to several partitions.  The workload measures the average produce latency, as well as the median, 95th percentile, and 99th percentile latency.
It can be configured to use a transactional producer which can commit transactions based on a set time interval or number of messages.

### RoundTripWorkload
RoundTripWorkload tests both production and consumption.  The workload starts a Kafka producer and consumer on a single node.  The consumer will read back the messages that were produced by the producer.

### ConsumeBench
ConsumeBench starts one or more Kafka consumers on a single agent node. Depending on the passed in configuration (see ConsumeBenchSpec), the consumers either subscribe to a set of topics (leveraging consumer group functionality and dynamic partition assignment) or manually assign partitions to themselves.
The workload measures the average consume latency, as well as the median, 95th percentile, and 99th percentile latency.

Faults
========================================
Trogdor can run several faults which deliberately break something in the cluster.

### ProcessStopFault
ProcessStopFault stops a process by sending it a SIGSTOP signal.  When the fault ends, the process is resumed with SIGCONT.

### NetworkPartitionFault
NetworkPartitionFault sets up an artificial network partition between one or more sets of nodes.  Currently, this is implemented using iptables.  The iptables rules are set up on the outbound traffic from the affected nodes.  Therefore, the affected nodes should still be reachable from outside the cluster.

External Processes
========================================
Trogdor supports running arbitrary commands in external processes. This is a generic way to run any configurable command in the Trogdor framework - be it a Python program, bash script, docker image, etc.

### ExternalCommandWorker
ExternalCommandWorker starts an external command defined by the ExternalCommandSpec. It essentially allows you to run any command on any Trogdor agent node.
The worker communicates with the external process via its stdin, stdout and stderr in a JSON protocol. It uses stdout for any actionable communication and only logs what it sees in stderr.
On startup the worker will first send a message describing the workload to the external process in this format:
```
{"id":<task ID string>, "workload":<configured workload JSON object>}
```
and will then listen for messages from the external process, again in a JSON format.
Said JSON can contain the following fields:
- status: If the object contains this field, the status of the worker will be set to the given value.
- error: If the object contains this field, the error of the worker will be set to the given value. Once an error occurs, the external process will be terminated.
- log: If the object contains this field, a log message will be issued with this text.
An example:
```json
{"log": "Finished successfully.", "status": {"p99ProduceLatency": "100ms", "messagesSent": 10000}}
```

Exec Mode
========================================
Sometimes, you just want to run a test quickly on a single node.  In this case, you can use "exec mode."  This mode allows you to run a single Trogdor Agent without a Coordinator.

When using exec mode, you must pass in a Task specification to use.  The Agent will try to start this task.

For example:

```
./bin/trogdor.sh agent -n node0 -c ./config/trogdor.conf --exec ./tests/spec/simple_produce_bench.json
```
When using exec mode, the Agent will exit once the task is complete.
