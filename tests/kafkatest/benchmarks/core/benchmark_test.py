# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.services.service import Service
from ducktape.tests.test import Test
from ducktape.mark import parametrize
from ducktape.mark import matrix

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.version import TRUNK, KafkaVersion
from kafkatest.services.performance import ProducerPerformanceService, EndToEndLatencyService, ConsumerPerformanceService, throughput, latency, compute_aggregate_throughput


TOPIC_REP_ONE = "topic-replication-factor-one"
TOPIC_REP_THREE = "topic-replication-factor-three"
DEFAULT_RECORD_SIZE = 100  # bytes


class Benchmark(Test):
    """A benchmark of Kafka producer/consumer performance. This replicates the test
    run here:
    https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines
    """
    def __init__(self, test_context):
        super(Benchmark, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 3
        self.topics = {
            TOPIC_REP_ONE: {'partitions': 6, 'replication-factor': 1},
            TOPIC_REP_THREE: {'partitions': 6, 'replication-factor': 3}
        }

        self.zk = ZookeeperService(test_context, self.num_zk)

        self.msgs_large = 10000000
        self.batch_size = 8*1024
        self.buffer_memory = 64*1024*1024
        self.msg_sizes = [10, 100, 1000, 10000, 100000]
        self.target_data_size = 128*1024*1024
        self.target_data_size_gb = self.target_data_size/float(1024*1024*1024)

    def setUp(self):
        self.zk.start()

    def start_kafka(self, security_protocol, interbroker_security_protocol, version):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics,
            version=version)
        self.kafka.log_level = "INFO"  # We don't DEBUG logging here
        self.kafka.start()

    @parametrize(acks=1, topic=TOPIC_REP_ONE)
    @parametrize(acks=1, topic=TOPIC_REP_THREE)
    @parametrize(acks=-1, topic=TOPIC_REP_THREE)
    @parametrize(acks=1, topic=TOPIC_REP_THREE, num_producers=3)
    @matrix(acks=[1], topic=[TOPIC_REP_THREE], message_size=[10, 100, 1000, 10000, 100000], compression_type=["none", "snappy"], security_protocol=['PLAINTEXT', 'SSL'])
    def test_producer_throughput(self, acks, topic, num_producers=1, message_size=DEFAULT_RECORD_SIZE,
                                 compression_type="none", security_protocol='PLAINTEXT', client_version=str(TRUNK),
                                 broker_version=str(TRUNK)):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Produce ~128MB worth of messages to a topic with 6 partitions. Required acks, topic replication factor,
        security protocol and message size are varied depending on arguments injected into this test.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.
        (This runs ProducerPerformance.java under the hood)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        self.start_kafka(security_protocol, security_protocol, broker_version)
        # Always generate the same total amount of data
        nrecords = int(self.target_data_size / message_size)

        self.producer = ProducerPerformanceService(
            self.test_context, num_producers, self.kafka, topic=topic,
            num_records=nrecords, record_size=message_size,  throughput=-1, version=client_version,
            settings={
                'acks': acks,
                'compression.type': compression_type,
                'batch.size': self.batch_size,
                'buffer.memory': self.buffer_memory})
        self.producer.run()
        return compute_aggregate_throughput(self.producer)

    @parametrize(security_protocol='SSL', interbroker_security_protocol='PLAINTEXT')
    @matrix(security_protocol=['PLAINTEXT', 'SSL'], compression_type=["none", "snappy"])
    def test_long_term_producer_throughput(self, compression_type="none", security_protocol='PLAINTEXT',
                                           interbroker_security_protocol=None, client_version=str(TRUNK),
                                           broker_version=str(TRUNK)):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Produce 10e6 100 byte messages to a topic with 6 partitions, replication-factor 3, and acks=1.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.

        (This runs ProducerPerformance.java under the hood)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version)
        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=self.msgs_large, record_size=DEFAULT_RECORD_SIZE,
            throughput=-1, version=client_version, settings={
                'acks': 1,
                'compression.type': compression_type,
                'batch.size': self.batch_size,
                'buffer.memory': self.buffer_memory
            },
            intermediate_stats=True
        )
        self.producer.run()

        summary = ["Throughput over long run, data > memory:"]
        data = {}
        # FIXME we should be generating a graph too
        # Try to break it into 5 blocks, but fall back to a smaller number if
        # there aren't even 5 elements
        block_size = max(len(self.producer.stats[0]) / 5, 1)
        nblocks = len(self.producer.stats[0]) / block_size

        for i in range(nblocks):
            subset = self.producer.stats[0][i*block_size:min((i+1)*block_size, len(self.producer.stats[0]))]
            if len(subset) == 0:
                summary.append(" Time block %d: (empty)" % i)
                data[i] = None
            else:
                records_per_sec = sum([stat['records_per_sec'] for stat in subset])/float(len(subset))
                mb_per_sec = sum([stat['mbps'] for stat in subset])/float(len(subset))

                summary.append(" Time block %d: %f rec/sec (%f MB/s)" % (i, records_per_sec, mb_per_sec))
                data[i] = throughput(records_per_sec, mb_per_sec)

        self.logger.info("\n".join(summary))
        return data

    @parametrize(security_protocol='SSL', interbroker_security_protocol='PLAINTEXT')
    @matrix(security_protocol=['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'], compression_type=["none", "snappy"])
    def test_end_to_end_latency(self, compression_type="none", security_protocol="PLAINTEXT",
                                interbroker_security_protocol=None, client_version=str(TRUNK),
                                broker_version=str(TRUNK)):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Produce (acks = 1) and consume 10e3 messages to a topic with 6 partitions and replication-factor 3,
        measuring the latency between production and consumption of each message.

        Return aggregate latency statistics.

        (Under the hood, this simply runs EndToEndLatency.scala)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version)
        self.logger.info("BENCHMARK: End to end latency")
        self.perf = EndToEndLatencyService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=10000,
            compression_type=compression_type, version=client_version
        )
        self.perf.run()
        return latency(self.perf.results[0]['latency_50th_ms'],  self.perf.results[0]['latency_99th_ms'], self.perf.results[0]['latency_999th_ms'])

    @parametrize(security_protocol='PLAINTEXT', new_consumer=False)
    @parametrize(security_protocol='SSL', interbroker_security_protocol='PLAINTEXT')
    @matrix(security_protocol=['PLAINTEXT', 'SSL'], compression_type=["none", "snappy"])
    def test_producer_and_consumer(self, compression_type="none", security_protocol="PLAINTEXT",
                                   interbroker_security_protocol=None, new_consumer=True,
                                   client_version=str(TRUNK), broker_version=str(TRUNK)):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Concurrently produce and consume 10e6 messages with a single producer and a single consumer,
        using new consumer if new_consumer == True

        Return aggregate throughput statistics for both producer and consumer.

        (Under the hood, this runs ProducerPerformance.java, and ConsumerPerformance.scala)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version)
        num_records = 10 * 1000 * 1000  # 10e6

        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE,
            num_records=num_records, record_size=DEFAULT_RECORD_SIZE, throughput=-1, version=client_version,
            settings={
                'acks': 1,
                'compression.type': compression_type,
                'batch.size': self.batch_size,
                'buffer.memory': self.buffer_memory
            }
        )
        self.consumer = ConsumerPerformanceService(
            self.test_context, 1, self.kafka, topic=TOPIC_REP_THREE, new_consumer=new_consumer, messages=num_records)
        Service.run_parallel(self.producer, self.consumer)

        data = {
            "producer": compute_aggregate_throughput(self.producer),
            "consumer": compute_aggregate_throughput(self.consumer)
        }
        summary = [
            "Producer + consumer:",
            str(data)]
        self.logger.info("\n".join(summary))
        return data

    @parametrize(security_protocol='PLAINTEXT', new_consumer=False)
    @parametrize(security_protocol='SSL', interbroker_security_protocol='PLAINTEXT')
    @matrix(security_protocol=['PLAINTEXT', 'SSL'], compression_type=["none", "snappy"])
    def test_consumer_throughput(self, compression_type="none", security_protocol="PLAINTEXT",
                                 interbroker_security_protocol=None, new_consumer=True, num_consumers=1,
                                 client_version=str(TRUNK), broker_version=str(TRUNK)):
        """
        Consume 10e6 100-byte messages with 1 or more consumers from a topic with 6 partitions
        (using new consumer iff new_consumer == True), and report throughput.
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version)
        num_records = 10 * 1000 * 1000  # 10e6

        # seed kafka w/messages
        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE,
            num_records=num_records, record_size=DEFAULT_RECORD_SIZE, throughput=-1, version=client_version,
            settings={
                'acks': 1,
                'compression.type': compression_type,
                'batch.size': self.batch_size,
                'buffer.memory': self.buffer_memory
            }
        )
        self.producer.run()

        # consume
        self.consumer = ConsumerPerformanceService(
            self.test_context, num_consumers, self.kafka,
            topic=TOPIC_REP_THREE, new_consumer=new_consumer, messages=num_records)
        self.consumer.group = "test-consumer-group"
        self.consumer.run()
        return compute_aggregate_throughput(self.consumer)

    def validate_versions(self, client_version, broker_version):
        assert client_version <= broker_version, "Client version %s should be <= than broker version %s" (client_version, broker_version)
