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
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.trogdor.degraded_network_fault_spec import DegradedNetworkFaultSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.utils.remote_account import network_device


class NetworkDegradeTest(Test):
    """
    This is a sanity test to make sure tc is behaving like we expect in our test environment
    """

    def __init__(self, test_context):
        super(NetworkDegradeTest, self).__init__(test_context)
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.trogdor = TrogdorService(context=self.test_context, client_services=[self.zk])

    def setUp(self):
        self.zk.start()
        self.trogdor.start()

    def teardown(self):
        self.trogdor.stop()
        self.zk.stop()

    @parametrize(task_name="latency-100", latency_ms=50, rate_limit_kbit=0)
    @parametrize(task_name="latency-100-rate-1000", latency_ms=50, rate_limit_kbit=1000)
    def test_latency(self, task_name, latency_ms, rate_limit_kbit):
        spec = DegradedNetworkFaultSpec(0, 10000, {})
        for node in self.zk.nodes:
            device = network_device(node)
            spec.add_node_spec(node.name, device, latency_ms, rate_limit_kbit)

        latency = self.trogdor.create_task(task_name, spec)

        zk0 = self.zk.nodes[0]
        zk1 = self.zk.nodes[1]

        r = re.compile(r".*time=(?P<time>[\d.]+)\sms.*")
        # PING ducker01 (172.24.0.2) 56(84) bytes of data.
        # 64 bytes from ducker01 (172.24.0.2): icmp_seq=1 ttl=64 time=0.325 ms
        # 64 bytes from ducker01 (172.24.0.2): icmp_seq=2 ttl=64 time=0.197 ms
        # 64 bytes from ducker01 (172.24.0.2): icmp_seq=3 ttl=64 time=0.145 ms
        times = []
        for line in zk0.account.ssh_capture("ping -i 1 -c 20 %s" % zk1.account.hostname):
            self.logger.debug("Ping output: %s" % line)
            m = r.match(line)
            if m is not None and m.group("time"):
                times.append(float(m.group("time")))
                self.logger.info("Parsed ping time of %d" % float(m.group("time")))

        # We expect to see some low ping times (before and after the task runs)
        # as well as high ping times (during the task)
        slow_times = [t for t in times if t > 100]
        fast_times = [t for t in times if t < 10]

        latency.stop()
        latency.wait_for_done()

        assert len(slow_times) > 8, "Expected to see more slow (>100ms) ping times"
        assert len(fast_times) > 8, "Expected to see more fast (<10ms) ping times"

    @parametrize(task_name="rate-1000", latency_ms=0, rate_limit_kbit=1000)
    @parametrize(task_name="rate-1000-latency-50", latency_ms=50, rate_limit_kbit=1000)
    def test_rate(self, task_name, latency_ms, rate_limit_kbit):
        zk0 = self.zk.nodes[0]
        zk1 = self.zk.nodes[1]

        spec = DegradedNetworkFaultSpec(0, 60000, {})
        device = network_device(zk0)
        spec.add_node_spec(zk0.name, device, latency_ms, rate_limit_kbit)

        # start the task and wait
        rate_limit = self.trogdor.create_task(task_name, spec)
        wait_until(lambda: rate_limit.running(),
                   timeout_sec=10,
                   err_msg="%s failed to start within 10 seconds." % rate_limit)

        iperf_server = zk1.account.ssh_capture("iperf -s")

        r = re.compile(r"^.*\s(?P<rate>[\d.]+)\sKbits/sec$")
        # [ ID] Interval       Transfer     Bandwidth
        # [  3]  0.0- 1.0 sec  2952576 KBytes  24187503 Kbits/sec
        # [  3]  1.0- 2.0 sec  2899072 KBytes  23749198 Kbits/sec
        # [  3]  2.0- 3.0 sec  2998784 KBytes  24566039 Kbits/sec
        measured_rates = []
        for line in zk0.account.ssh_capture("iperf -i 1 -t 20 -f k -c %s" % zk1.account.hostname):
            self.logger.info("iperf output %s" % line)
            m = r.match(line)
            if m is not None:
                measured_rate = float(m.group("rate"))
                measured_rates.append(measured_rate)
                self.logger.info("Parsed rate of %d kbit/s from iperf" % measured_rate)

        zk1.account.kill_process("iperf")
        # consume the output
        for _ in iperf_server:
            continue

        rate_limit.stop()
        rate_limit.wait_for_done()

        self.logger.info("Measured rates: %s" % measured_rates)
        acceptable_rates = [r for r in measured_rates if rate_limit_kbit / 10 < r < rate_limit_kbit * 10]

        msg = "Expected most of the measured rates to be within an order of magnitude of target %d." % rate_limit_kbit
        msg += " This means `tc` did not limit the bandwidth as expected."
        assert len(acceptable_rates) > 5, msg
