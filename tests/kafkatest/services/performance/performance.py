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
