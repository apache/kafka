Kafka Raft
=================
Kafka Raft is a sub module of Apache Kafka which features a tailored version of
[Raft Consensus Protocol](https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf).
<p>

Eventually this module will be integrated into the Kafka server. For now,
we have a standalone test server which can be used for performance testing.
Below we describe the details to set this up.

### Run Single Quorum ###
    bin/test-raft-server-start.sh config/raft.properties

### Run Multi Node Quorum ###
Create 3 separate raft quorum properties as the following:

`cat << EOF >> config/raft-quorum-1.properties`
    
    broker.id=1
    listeners=PLAINTEXT://localhost:9092
    controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/raft-logs-1
    EOF

`cat << EOF >> config/raft-quorum-2.properties`
    
    broker.id=2
    listeners=PLAINTEXT://localhost:9093
    controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/raft-logs-2
    EOF
    
`cat << EOF >> config/raft-quorum-3.properties`
    
    broker.id=3
    listeners=PLAINTEXT://localhost:9094
    controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/raft-logs-3
    EOF
 
Open up 3 separate terminals, and run individual commands:

    bin/test-raft-server-start.sh --config config/raft-quorum-1.properties
    bin/test-raft-server-start.sh --config config/raft-quorum-2.properties
    bin/test-raft-server-start.sh --config config/raft-quorum-3.properties

Once a leader is elected, it will begin writing to an internal
`__cluster_metadata` topic with a steady workload of random data.
You can control the workload using the `--throughput` and `--record-size`
arguments passed to `test-raft-server-start.sh`.
