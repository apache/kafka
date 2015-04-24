# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.services.service import Service

from tests.test import KafkaTest
from services.performance import ProducerPerformanceService, ConsumerPerformanceService, \
    EndToEndLatencyService


class KafkaBenchmark(KafkaTest):
    '''A benchmark of Kafka producer/consumer performance. This replicates the test
    run here:
    https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines
    '''
    def __init__(self, test_context):
        super(KafkaBenchmark, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            'test-rep-one' : { 'partitions': 6, 'replication-factor': 1 },
            'test-rep-three' : { 'partitions': 6, 'replication-factor': 3 }
        })

    def run(self):
        msgs_default = 50000000
        msgs_large = 100000000
        msg_size_default = 100
        batch_size = 8*1024
        buffer_memory = 64*1024*1024
        msg_sizes = [10, 100, 1000, 10000, 100000]
        target_data_size = 1024*1024*1024
        target_data_size_gb = target_data_size/float(1024*1024*1024)
        # These settings will work in the default local Vagrant VMs, useful for testing
        if False:
            msgs_default = 1000000
            msgs_large = 10000000
            msg_size_default = 100
            batch_size = 8*1024
            buffer_memory = 64*1024*1024
            msg_sizes = [10, 100, 1000, 10000, 100000]
            target_data_size = 128*1024*1024
            target_data_size_gb = target_data_size/float(1024*1024*1024)

        # PRODUCER TESTS

        self.logger.info("BENCHMARK: Single producer, no replication")
        single_no_rep = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-one", num_records=msgs_default, record_size=msg_size_default, throughput=-1,
            settings={'acks':1, 'batch.size':batch_size, 'buffer.memory':buffer_memory}
        )
        single_no_rep.run()

        self.logger.info("BENCHMARK: Single producer, async 3x replication")
        single_rep_async = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=msgs_default, record_size=msg_size_default, throughput=-1,
            settings={'acks':1, 'batch.size':batch_size, 'buffer.memory':buffer_memory}
        )
        single_rep_async.run()

        self.logger.info("BENCHMARK: Single producer, sync 3x replication")
        single_rep_sync = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=msgs_default, record_size=msg_size_default, throughput=-1,
            settings={'acks':-1, 'batch.size':batch_size, 'buffer.memory':buffer_memory}
        )
        single_rep_sync.run()

        self.logger.info("BENCHMARK: Three producers, async 3x replication")
        three_rep_async = ProducerPerformanceService(
            self.service_context(3), self.kafka,
            topic="test-rep-three", num_records=msgs_default, record_size=msg_size_default, throughput=-1,
            settings={'acks':1, 'batch.size':batch_size, 'buffer.memory':buffer_memory}
        )
        three_rep_async.run()


        msg_size_perf = {}
        for msg_size in msg_sizes:
            self.logger.info("BENCHMARK: Message size %d (%f GB total, single producer, async 3x replication)", msg_size, target_data_size_gb)
            # Always generate the same total amount of data
            nrecords = int(target_data_size / msg_size)
            perf = ProducerPerformanceService(
                self.service_context(1), self.kafka,
                topic="test-rep-three", num_records=nrecords, record_size=msg_size, throughput=-1,
                settings={'acks':1, 'batch.size':batch_size, 'buffer.memory':buffer_memory}
            )
            perf.run()
            msg_size_perf[msg_size] = perf

        # CONSUMER TESTS

        # All consumer tests use the messages from the first benchmark, so
        # they'll get messages of the default message size
        self.logger.info("BENCHMARK: Single consumer")
        single_consumer = ConsumerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=msgs_default, throughput=-1, threads=1
        )
        single_consumer.run()

        self.logger.info("BENCHMARK: Three consumers")
        three_consumers = ConsumerPerformanceService(
            self.service_context(3), self.kafka,
            topic="test-rep-three", num_records=msgs_default, throughput=-1, threads=1
        )
        three_consumers.run()

        # PRODUCER + CONSUMER TEST
        self.logger.info("BENCHMARK: Producer + Consumer")
        pc_producer = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=msgs_default, record_size=msg_size_default, throughput=-1,
            settings={'acks':1, 'batch.size':batch_size, 'buffer.memory':buffer_memory}
        )
        pc_consumer = ConsumerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=msgs_default, throughput=-1, threads=1
        )
        Service.run_parallel(pc_producer, pc_consumer)

        # END TO END LATENCY TEST
        self.logger.info("BENCHMARK: End to end latency")
        e2e_latency = EndToEndLatencyService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=10000
        )
        e2e_latency.run()


        # LONG TERM THROUGHPUT TEST

        # Because of how much space this ends up using, we clear out the
        # existing cluster to start from a clean slate. This also keeps us from
        # running out of space with limited disk space.
        self.tearDown()
        self.setUp()
        self.logger.info("BENCHMARK: Long production")
        throughput_perf = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-three", num_records=msgs_large, record_size=msg_size_default, throughput=-1,
            settings={'acks':1, 'batch.size':batch_size, 'buffer.memory':buffer_memory},
            intermediate_stats=True
        )
        throughput_perf.run()

        # Summarize, extracting just the key info. With multiple
        # producers/consumers, we display the aggregate value
        def throughput(perf):
            aggregate_rate = sum([r['records_per_sec'] for r in perf.results])
            aggregate_mbps = sum([r['mbps'] for r in perf.results])
            return "%f rec/sec (%f MB/s)" % (aggregate_rate, aggregate_mbps)
        self.logger.info("=================")
        self.logger.info("BENCHMARK RESULTS")
        self.logger.info("=================")
        self.logger.info("Single producer, no replication: %s", throughput(single_no_rep))
        self.logger.info("Single producer, async 3x replication: %s", throughput(single_rep_async))
        self.logger.info("Single producer, sync 3x replication: %s", throughput(single_rep_sync))
        self.logger.info("Three producers, async 3x replication: %s", throughput(three_rep_async))
        self.logger.info("Message size:")
        for msg_size in msg_sizes:
            self.logger.info(" %d: %s", msg_size, throughput(msg_size_perf[msg_size]))
        self.logger.info("Throughput over long run, data > memory:")
        # FIXME we should be generating a graph too
        # Try to break it into 5 blocks, but fall back to a smaller number if
        # there aren't even 5 elements
        block_size = max(len(throughput_perf.stats[0]) / 5, 1)
        nblocks = len(throughput_perf.stats[0]) / block_size
        for i in range(nblocks):
            subset = throughput_perf.stats[0][i*block_size:min((i+1)*block_size,len(throughput_perf.stats[0]))]
            if len(subset) == 0:
                self.logger.info(" Time block %d: (empty)", i)
            else:
                self.logger.info(" Time block %d: %f rec/sec (%f MB/s)", i,
                                 sum([stat['records_per_sec'] for stat in subset])/float(len(subset)),
                                 sum([stat['mbps'] for stat in subset])/float(len(subset))
                             )
        self.logger.info("Single consumer: %s", throughput(single_consumer))
        self.logger.info("Three consumers: %s", throughput(three_consumers))
        self.logger.info("Producer + consumer:")
        self.logger.info(" Producer: %s", throughput(pc_producer))
        self.logger.info(" Consumer: %s", throughput(pc_producer))
        self.logger.info("End-to-end latency: median %f ms, 99%% %f ms, 99.9%% %f ms", e2e_latency.results[0]['latency_50th_ms'], e2e_latency.results[0]['latency_99th_ms'], e2e_latency.results[0]['latency_999th_ms'])
