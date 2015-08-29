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

from ducktape.services.background_thread import BackgroundThreadService


class PerformanceService(BackgroundThreadService):
    def __init__(self, context, num_nodes):
        super(PerformanceService, self).__init__(context, num_nodes)
        self.results = [None] * self.num_nodes
        self.stats = [[] for x in range(self.num_nodes)]

    def clean_node(self, node):
        node.account.kill_process("java", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/*", allow_fail=False)


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
