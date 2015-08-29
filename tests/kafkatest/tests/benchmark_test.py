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
from ducktape.mark import parametrize
from ducktape.mark import matrix

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.performance import ProducerPerformanceService, EndToEndLatencyService, ConsumerPerformanceService


TOPIC_REP_ONE = "topic-replication-factor-one"
TOPIC_REP_THREE = "topic-replication-factor-three"
DEFAULT_RECORD_SIZE = 100  # bytes


class Benchmark(KafkaTest):
    """A benchmark of Kafka producer/consumer performance. This replicates the test
    run here:
    https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines
    """
    def __init__(self, test_context):
        super(Benchmark, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            TOPIC_REP_ONE: {'partitions': 6, 'replication-factor': 1},
            TOPIC_REP_THREE: {'partitions': 6, 'replication-factor': 3}
        })

        if True:
            # Works on both aws and local
            self.msgs = 1000000
            self.msgs_default = 1000000
        else:
            # Can use locally on Vagrant VMs, but may use too much memory for aws
            self.msgs = 50000000
            self.msgs_default = 50000000

        self.msgs_large = 10000000
        self.batch_size = 8*1024
        self.buffer_memory = 64*1024*1024
        self.msg_sizes = [10, 100, 1000, 10000, 100000]
        self.target_data_size = 128*1024*1024
        self.target_data_size_gb = self.target_data_size/float(1024*1024*1024)

    @parametrize(acks=1, topic=TOPIC_REP_ONE, num_producers=1, message_size=DEFAULT_RECORD_SIZE)
    @parametrize(acks=1, topic=TOPIC_REP_THREE, num_producers=1, message_size=DEFAULT_RECORD_SIZE)
    @parametrize(acks=-1, topic=TOPIC_REP_THREE, num_producers=1, message_size=DEFAULT_RECORD_SIZE)
    @parametrize(acks=1, topic=TOPIC_REP_THREE, num_producers=3, message_size=DEFAULT_RECORD_SIZE)
    def test_producer_throughput(self, acks, topic, num_producers, message_size):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Produce 1e6 100-byte messages to a topic with 6 partitions. Required acks, and topic replication factor
        are varied depending on arguments injected into this test.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.

        (This runs ProducerPerformance.java under the hood)
        """
        self.perf = ProducerPerformanceService(
            self.test_context, num_producers, self.kafka, topic=topic,
            num_records=self.msgs_default, record_size=message_size,  throughput=-1,
            settings={
                'acks': acks,
                'batch.size': self.batch_size,
                'buffer.memory': self.buffer_memory})
        self.perf.run()
        return compute_aggregate_throughput(self.perf)

    @matrix(message_size=[10, 100, 1000, 10000, 100000])
    def test_producer_throughput_multiple_message_size(self, message_size):
        """
        Setup: 1 node zk + 3 node kafka cluster
        With a single producer, produce a fixed amount of data to a topic with 6 partitions, acks=1,
        replication-factor=3, and message size depends on the value of the injected argument.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.

        (This runs ProducerPerformance.java under the hood)
        """
        # Always generate the same total amount of data
        nrecords = int(self.target_data_size / message_size)

        self.perf = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=nrecords, record_size=message_size, throughput=-1,
            settings={'acks': 1, 'batch.size': self.batch_size, 'buffer.memory': self.buffer_memory}
        )
        self.perf.run()
        return compute_aggregate_throughput(self.perf)

    def test_long_term_producer_throughput(self):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Produce 10e6 100 byte messages to a topic with 6 partitions, replication-factor 3, and acks=1.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.

        (This runs ProducerPerformance.java under the hood)
        """
        self.perf = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=self.msgs_large, record_size=DEFAULT_RECORD_SIZE,
            throughput=-1, settings={'acks': 1, 'batch.size': self.batch_size, 'buffer.memory': self.buffer_memory},
            intermediate_stats=True
        )
        self.perf.run()

        summary = ["Throughput over long run, data > memory:"]
        data = {}
        # FIXME we should be generating a graph too
        # Try to break it into 5 blocks, but fall back to a smaller number if
        # there aren't even 5 elements
        block_size = max(len(self.perf.stats[0]) / 5, 1)
        nblocks = len(self.perf.stats[0]) / block_size
        for i in range(nblocks):
            subset = self.perf.stats[0][i*block_size:min((i+1)*block_size, len(self.perf.stats[0]))]
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

    def test_end_to_end_latency(self):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Produce (acks = 1) and consume 10e3 messages to a topic with 6 partitions and replication-factor 3,
        measuring the latency between production and consumption of each message.

        Return aggregate latency statistics.

        (Under the hood, this simply runs EndToEndLatency.scala)
        """
        self.logger.info("BENCHMARK: End to end latency")
        self.perf = EndToEndLatencyService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=10000
        )
        self.perf.run()
        return latency(self.perf.results[0]['latency_50th_ms'],  self.perf.results[0]['latency_99th_ms'], self.perf.results[0]['latency_999th_ms'])

    @matrix(new_consumer=[True, False])
    def test_producer_and_consumer(self, new_consumer=False):
        """
        Setup: 1 node zk + 3 node kafka cluster
        Concurrently produce and consume 1e6 messages with a single producer and a single consumer,
        using new consumer if new_consumer == True

        Return aggregate throughput statistics for both producer and consumer.

        (Under the hood, this runs ProducerPerformance.java, and ConsumerPerformance.scala)
        """

        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=self.msgs_default, record_size=DEFAULT_RECORD_SIZE, throughput=-1,
            settings={'acks': 1, 'batch.size': self.batch_size, 'buffer.memory': self.buffer_memory}
        )
        self.consumer = ConsumerPerformanceService(
            self.test_context, 1, self.kafka, topic=TOPIC_REP_THREE, new_consumer=new_consumer, messages=self.msgs_default)
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

    @matrix(new_consumer=[True, False])
    @matrix(num_consumers=[1, 3])
    def test_single_consumer(self, new_consumer, num_consumers):
        """
        Consume 1e6 100-byte messages with 1 or more consumers from a topic with 6 partitions
        (using new consumer iff new_consumer == True), and report throughput.
        """
        # seed kafka w/messages
        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=self.msgs_default, record_size=DEFAULT_RECORD_SIZE, throughput=-1,
            settings={'acks': 1, 'batch.size': self.batch_size, 'buffer.memory': self.buffer_memory}
        )
        self.producer.run()

        # consume
        self.perf = ConsumerPerformanceService(
            self.test_context, num_consumers, self.kafka,
            topic=TOPIC_REP_THREE, new_consumer=new_consumer, messages=self.msgs_default)
        self.perf.run()
        return compute_aggregate_throughput(self.perf)


def throughput(records_per_sec, mb_per_sec):
    """Helper method to ensure uniform representation of throughput data"""
    return {
        "records_per_sec": records_per_sec,
        "mb_per_sec": mb_per_sec
    }


def latency(latency_50th_ms, latency_99th_ms, latency_999th_ms):
    """Helper method to ensure uniform representation of latency data"""
    return {
        "latency_50th_ms": latency_50th_ms,
        "latency_99th_ms": latency_99th_ms,
        "latency_999th_ms": latency_999th_ms
    }


def compute_aggregate_throughput(perf):
    """Helper method for computing throughput after running a performance service."""
    aggregate_rate = sum([r['records_per_sec'] for r in perf.results])
    aggregate_mbps = sum([r['mbps'] for r in perf.results])

    return throughput(aggregate_rate, aggregate_mbps)
