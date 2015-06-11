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

from ducktape.services.background_thread import BackgroundThreadService


class PerformanceService(BackgroundThreadService):
    def __init__(self, context, num_nodes):
        super(PerformanceService, self).__init__(context, num_nodes)
        self.results = [None] * self.num_nodes
        self.stats = [[] for x in range(self.num_nodes)]


class ProducerPerformanceService(PerformanceService):
    def __init__(self, context, num_nodes, kafka, topic, num_records, record_size, throughput, settings={}, intermediate_stats=False):
        super(ProducerPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.args = {
            'topic': topic,
            'num_records': num_records,
            'record_size': record_size,
            'throughput': throughput
        }
        self.settings = settings
        self.intermediate_stats = intermediate_stats

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({'bootstrap_servers': self.kafka.bootstrap_servers()})
        cmd = "/opt/kafka/bin/kafka-run-class.sh org.apache.kafka.clients.tools.ProducerPerformance "\
              "%(topic)s %(num_records)d %(record_size)d %(throughput)d bootstrap.servers=%(bootstrap_servers)s" % args

        for key,value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))
        self.logger.debug("Producer performance %d command: %s", idx, cmd)

        def parse_stats(line):
            parts = line.split(',')
            return {
                'records': int(parts[0].split()[0]),
                'records_per_sec': float(parts[1].split()[0]),
                'mbps': float(parts[1].split('(')[1].split()[0]),
                'latency_avg_ms': float(parts[2].split()[0]),
                'latency_max_ms': float(parts[3].split()[0]),
                'latency_50th_ms': float(parts[4].split()[0]),
                'latency_95th_ms': float(parts[5].split()[0]),
                'latency_99th_ms': float(parts[6].split()[0]),
                'latency_999th_ms': float(parts[7].split()[0]),
            }
        last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("Producer performance %d: %s", idx, line.strip())
            if self.intermediate_stats:
                try:
                    self.stats[idx-1].append(parse_stats(line))
                except:
                    # Sometimes there are extraneous log messages
                    pass
            last = line
        try:
            self.results[idx-1] = parse_stats(last)
        except:
            self.logger.error("Bad last line: %s", last)


class ConsumerPerformanceService(PerformanceService):
    def __init__(self, context, num_nodes, kafka, topic, num_records, throughput, threads=1, settings={}):
        super(ConsumerPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.args = {
            'topic': topic,
            'num_records': num_records,
            'throughput': throughput,
            'threads': threads,
        }
        self.settings = settings

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({'zk_connect': self.kafka.zk.connect_setting()})
        cmd = "/opt/kafka/bin/kafka-consumer-perf-test.sh "\
              "--topic %(topic)s --messages %(num_records)d --zookeeper %(zk_connect)s" % args
        for key,value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))
        self.logger.debug("Consumer performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("Consumer performance %d: %s", idx, line.strip())
            last = line
        # Parse and save the last line's information
        parts = last.split(',')

        print "=" * 20
        print "ConsumerPerformanceService data:"
        print parts
        print "-" * 20

        self.results[idx-1] = {
            'total_mb': float(parts[3]),
            'mbps': float(parts[4]),
            'records_per_sec': float(parts[5]),
        }


class EndToEndLatencyService(PerformanceService):
    def __init__(self, context, num_nodes, kafka, topic, num_records, consumer_fetch_max_wait=100, acks=1):
        super(EndToEndLatencyService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.args = {
            'topic': topic,
            'num_records': num_records,
            'consumer_fetch_max_wait': consumer_fetch_max_wait,
            'acks': acks
        }

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({
            'zk_connect': self.kafka.zk.connect_setting(),
            'bootstrap_servers': self.kafka.bootstrap_servers(),
        })
        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.TestEndToEndLatency "\
              "%(bootstrap_servers)s %(zk_connect)s %(topic)s %(num_records)d "\
              "%(consumer_fetch_max_wait)d %(acks)d" % args
        self.logger.debug("End-to-end latency %d command: %s", idx, cmd)
        results = {}
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("End-to-end latency %d: %s", idx, line.strip())
            if line.startswith("Avg latency:"):
                results['latency_avg_ms'] = float(line.split()[2])
            if line.startswith("Percentiles"):
                results['latency_50th_ms'] = float(line.split()[3][:-1])
                results['latency_99th_ms'] = float(line.split()[6][:-1])
                results['latency_999th_ms'] = float(line.split()[9])
        self.results[idx-1] = results


def parse_performance_output(summary):
        parts = summary.split(',')
        results = {
            'records': int(parts[0].split()[0]),
            'records_per_sec': float(parts[1].split()[0]),
            'mbps': float(parts[1].split('(')[1].split()[0]),
            'latency_avg_ms': float(parts[2].split()[0]),
            'latency_max_ms': float(parts[3].split()[0]),
            'latency_50th_ms': float(parts[4].split()[0]),
            'latency_95th_ms': float(parts[5].split()[0]),
            'latency_99th_ms': float(parts[6].split()[0]),
            'latency_999th_ms': float(parts[7].split()[0]),
        }
        # To provide compatibility with ConsumerPerformanceService
        results['total_mb'] = results['mbps'] * (results['records'] / results['records_per_sec'])
        results['rate_mbps'] = results['mbps']
        results['rate_mps'] = results['records_per_sec']

        return results
