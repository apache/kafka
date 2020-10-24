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
    quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/raft-logs-1
    
    zookeeper.connect=localhost:2181
    EOF

`cat << EOF >> config/raft-quorum-2.properties`
    
    broker.id=2
    listeners=PLAINTEXT://localhost:9093
    quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/raft-logs-2
    
    zookeeper.connect=localhost:2181
    EOF
    
`cat << EOF >> config/raft-quorum-3.properties`
    
    broker.id=3
    listeners=PLAINTEXT://localhost:9094
    quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
    log.dirs=/tmp/raft-logs-3
    
    zookeeper.connect=localhost:2181
    EOF
 
Open up 3 separate terminals, and run individual commands:

    bin/test-raft-server-start.sh config/raft-quorum-1.properties
    bin/test-raft-server-start.sh config/raft-quorum-2.properties
    bin/test-raft-server-start.sh config/raft-quorum-3.properties
    
This would setup a three node Raft quorum with node id 1,2,3 using different endpoints and log dirs. 

### Simulate a distributed state machine ###
You need to use a `VerifiableProducer` to produce monolithic increasing records to the replicated state machine.

    ./bin/kafka-run-class.sh org.apache.kafka.tools.VerifiableProducer --bootstrap-server http://localhost:9092 \
    --topic __cluster_metadata --max-messages 2000 --throughput 1 --producer.config config/producer.properties
### Run Performance Test ###
Run the `ProducerPerformance` module using this example command:

    ./bin/kafka-producer-perf-test.sh --topic __cluster_metadata --num-records 2000 --throughput -1 --record-size 10 --producer.config config/producer.properties 

