KRaft (Kafka Raft)
==================
KRaft (Kafka Raft) is a protocol based on the [Raft Consensus Protocol](https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf)
tailored for Apache Kafka.

This is used by Apache Kafka in the [KRaft (Kafka Raft Metadata) mode](https://github.com/apache/kafka/blob/trunk/config/kraft/README.md). We
also have a standalone test server which can be used for performance testing. We describe the details to set this up below.

### Run Single Quorum ###
    bin/test-kraft-server-start.sh --config config/kraft.properties

### Run Multi Node Quorum ###
Create 3 separate KRaft quorum properties as the following:

`cat << EOF >> config/kraft-quorum-1.properties`
    
    node.id=1
    listeners=PLAINTEXT://localhost:9092
    controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/kraft-logs-1
    EOF

`cat << EOF >> config/kraft-quorum-2.properties`
    
    node.id=2
    listeners=PLAINTEXT://localhost:9093
    controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/kraft-logs-2
    EOF
    
`cat << EOF >> config/kraft-quorum-3.properties`
    
    node.id=3
    listeners=PLAINTEXT://localhost:9094
    controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/kraft-logs-3
    EOF
 
Open up 3 separate terminals, and run individual commands:

    bin/test-kraft-server-start.sh --config config/kraft-quorum-1.properties
    bin/test-kraft-server-start.sh --config config/kraft-quorum-2.properties
    bin/test-kraft-server-start.sh --config config/kraft-quorum-3.properties

Once a leader is elected, it will begin writing to an internal
`__raft_performance_test` topic with a steady workload of random data.
You can control the workload using the `--throughput` and `--record-size`
arguments passed to `test-kraft-server-start.sh`.
