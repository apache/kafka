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

import re

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.trogdor.degraded_network_fault_spec import DegradedNetworkFaultSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.kafka import KafkaService, quorum


class NetworkDegradeTest(Test):
    """
    These tests ensure that the network degrade Trogdor specs (which use "tc") are working as expected in whatever
    environment the system tests may be running in. The linux tools "ping" and "iperf" are used for validation
    and need to be available along with "tc" in the test environment.
    """

    def __init__(self, test_context):
        super(NetworkDegradeTest, self).__init__(test_context)
        self.kafka = KafkaService(test_context, num_nodes=2, zk=None, controller_num_nodes_override=2)
        self.trogdor = TrogdorService(context=self.test_context, client_services=[self.kafka.controller_quorum])

    def setUp(self):
        self.kafka.start()
        self.trogdor.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()

    @cluster(num_nodes=3)
    @parametrize(task_name="latency-100", device_name="eth0", latency_ms=50, rate_limit_kbit=0, metadata_quorum=quorum.combined_kraft)
    @parametrize(task_name="latency-100-rate-1000", device_name="eth0", latency_ms=50, rate_limit_kbit=1000, metadata_quorum=quorum.combined_kraft)
    def test_latency(self, task_name, device_name, latency_ms, rate_limit_kbit, metadata_quorum=quorum.combined_kraft):
        spec = DegradedNetworkFaultSpec(0, 10000)
        for node in self.kafka.controller_quorum.nodes:
            spec.add_node_spec(node.name, device_name, latency_ms, rate_limit_kbit)

        latency = self.trogdor.create_task(task_name, spec)

        quorum0 = self.kafka.controller_quorum.nodes[0]
        quorum1 = self.kafka.controller_quorum.nodes[1]
        

        # Capture the ping times from the ping stdout
        # 64 bytes from ducker01 (172.24.0.2): icmp_seq=1 ttl=64 time=0.325 ms
        r = re.compile(r".*time=(?P<time>[\d.]+)\sms.*")

        times = []
        for line in quorum0.account.ssh_capture("ping -i 1 -c 20 %s" % quorum1.account.hostname):
            self.logger.debug("Ping output: %s" % line)
            m = r.match(line)
            if m is not None and m.group("time"):
                times.append(float(m.group("time")))
                self.logger.info("Parsed ping time of %d" % float(m.group("time")))
        self.logger.debug("Captured ping times: %s" % times)

        # We expect to see some low ping times (before and after the task runs) as well as high ping times
        # (during the task). For the high time, it's twice the configured latency since both links apply the
        # rule, 80% for a little variance buffer
        high_time_ms = 0.8 * 2 * latency_ms
        low_time_ms = 10
        slow_times = [t for t in times if t > high_time_ms]
        fast_times = [t for t in times if t < low_time_ms]

        latency.stop()
        latency.wait_for_done()

        # We captured 20 ping times. Assert that at least 5 were "fast" and 5 were "slow"
        assert len(slow_times) > 5, "Expected to see more slow ping times (lower than %d)" % low_time_ms
        assert len(fast_times) > 5, "Expected to see more fast ping times (higher than %d)" % high_time_ms

    @cluster(num_nodes=3)
    @parametrize(task_name="rate-1000", device_name="eth0", latency_ms=0, rate_limit_kbit=1000000, metadata_quorum=quorum.combined_kraft)
    @parametrize(task_name="rate-1000-latency-50", device_name="eth0", latency_ms=50, rate_limit_kbit=1000000, metadata_quorum=quorum.combined_kraft)
    def test_rate(self, task_name, device_name, latency_ms, rate_limit_kbit, metadata_quorum=quorum.combined_kraft):
        quorum0 = self.kafka.controller_quorum.nodes[0]
        quorum1 = self.kafka.controller_quorum.nodes[1]

        spec = DegradedNetworkFaultSpec(0, 60000)
        spec.add_node_spec(quorum0.name, device_name, latency_ms, rate_limit_kbit)

        # start the task and wait
        rate_limit = self.trogdor.create_task(task_name, spec)
        wait_until(lambda: rate_limit.running(),
                   timeout_sec=10,
                   err_msg="%s failed to start within 10 seconds." % rate_limit)

        # Run iperf server on quorum1, iperf client on quorum0
        iperf_server = quorum1.account.ssh_capture("iperf -s")

        # Wait until iperf server is listening before starting the client
        for line in iperf_server:
          self.logger.debug("iperf server output %s" % line)
          if "server listening" in line.lower():
            self.logger.info("iperf server is ready")
            break

        # Capture the measured kbps between the two nodes.
        # [  3]  0.0- 1.0 sec  2952576 KBytes  24187503 Kbits/sec
        r = re.compile(r"^.*\s(?P<rate>[\d.]+)\sKbits/sec$")

        measured_rates = []
        for line in quorum0.account.ssh_capture("iperf -i 1 -t 20 -f k -c %s" % quorum1.account.hostname):
            self.logger.info("iperf output %s" % line)
            m = r.match(line)
            if m is not None:
                measured_rate = float(m.group("rate"))
                measured_rates.append(measured_rate)
                self.logger.info("Parsed rate of %d kbit/s from iperf" % measured_rate)

        # kill iperf server and consume the stdout to ensure clean exit
        quorum1.account.kill_process("iperf")
        for _ in iperf_server:
            continue

        rate_limit.stop()
        rate_limit.wait_for_done()

        self.logger.info("Measured rates: %s" % measured_rates)

        # We expect to see measured rates within an order of magnitude of our target rate
        low_kbps = rate_limit_kbit // 10
        high_kbps = rate_limit_kbit * 10
        acceptable_rates = [r for r in measured_rates if low_kbps < r < high_kbps]

        msg = "Expected most of the measured rates to be within an order of magnitude of target %d." % rate_limit_kbit
        msg += " This means `tc` did not limit the bandwidth as expected."
        assert len(acceptable_rates) > 5, msg
