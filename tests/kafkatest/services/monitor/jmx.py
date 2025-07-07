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

import os

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.version import get_version, V_3_4_0, DEV_BRANCH

class JmxMixin(object):
    """This mixin helps existing service subclasses start JmxTool on their worker nodes and collect jmx stats.

    A couple things worth noting:
    - this is not a service in its own right.
    - we assume the service using JmxMixin also uses KafkaPathResolverMixin
    - this uses the --wait option for JmxTool, so the list of object names must be explicit; no patterns are permitted
    """
    def __init__(self, num_nodes, jmx_object_names=None, jmx_attributes=None, jmx_poll_ms=1000, root="/mnt"):
        self.jmx_object_names = jmx_object_names
        self.jmx_attributes = jmx_attributes or []
        self.jmx_poll_ms = jmx_poll_ms
        self.jmx_port = 9192

        self.started = [False] * num_nodes
        self.jmx_stats = [{} for x in range(num_nodes)]
        self.maximum_jmx_value = {}  # map from object_attribute_name to maximum value observed over time
        self.average_jmx_value = {}  # map from object_attribute_name to average value observed over time

        self.jmx_tool_log = os.path.join(root, "jmx_tool.log")
        self.jmx_tool_err_log = os.path.join(root, "jmx_tool.err.log")

    def clean_node(self, node, idx=None):
        node.account.kill_java_processes(self.jmx_class_name(self.jmxtool_version(node)), clean_shutdown=False,
                                         allow_fail=True)
        if idx is None:
            idx = self.idx(node)
        self.started[idx-1] = False
        node.account.ssh("rm -f -- %s %s" % (self.jmx_tool_log, self.jmx_tool_err_log), allow_fail=False)

    def start_jmx_tool(self, idx, node):
        if self.jmx_object_names is None:
            self.logger.debug("%s: Not starting jmx tool because no jmx objects are defined" % node.account)
            return

        if self.started[idx-1]:
            self.logger.debug("%s: jmx tool has been started already on this node" % node.account)
            return

        # JmxTool is not particularly robust to slow-starting processes. In order to ensure JmxTool doesn't fail if the
        # process we're trying to monitor takes awhile before listening on the JMX port, wait until we can see that port
        # listening before even launching JmxTool
        def check_jmx_port_listening():
            return 0 == node.account.ssh("nc -z 127.0.0.1 %d" % self.jmx_port, allow_fail=True)

        wait_until(check_jmx_port_listening, timeout_sec=30, backoff_sec=.1,
                   err_msg="%s: Never saw JMX port for %s start listening" % (node.account, self))
        use_jmxtool_version = self.jmxtool_version(node)
        cmd = "%s %s " % (self.path.script("kafka-run-class.sh", use_jmxtool_version), self.jmx_class_name(use_jmxtool_version))
        cmd += "--reporting-interval %d --jmx-url service:jmx:rmi:///jndi/rmi://127.0.0.1:%d/jmxrmi" % (self.jmx_poll_ms, self.jmx_port)
        cmd += " --wait"
        for jmx_object_name in self.jmx_object_names:
            cmd += " --object-name %s" % jmx_object_name
        cmd += " --attributes "
        for jmx_attribute in self.jmx_attributes:
            cmd += "%s," % jmx_attribute
        cmd += " 1>> %s" % self.jmx_tool_log
        cmd += " 2>> %s &" % self.jmx_tool_err_log

        self.logger.debug("%s: Start JmxTool %d command: %s" % (node.account, idx, cmd))
        node.account.ssh(cmd, allow_fail=False)
        wait_until(lambda: self._jmx_has_output(node), timeout_sec=30, backoff_sec=.5, err_msg="%s: Jmx tool took too long to start" % node.account)
        self.started[idx-1] = True

    def _jmx_has_output(self, node):
        """Helper used as a proxy to determine whether jmx is running by that jmx_tool_log contains output."""
        try:
            node.account.ssh("test -s %s" % self.jmx_tool_log, allow_fail=False)
            return True
        except RemoteCommandError:
            return False

    def read_jmx_output(self, idx, node):
        if not self.started[idx-1]:
            return

        object_attribute_names = []

        cmd = "cat %s" % self.jmx_tool_log
        self.logger.debug("Read jmx output %d command: %s", idx, cmd)
        lines = [line for line in node.account.ssh_capture(cmd, allow_fail=False)]
        assert len(lines) > 1, "There don't appear to be any samples in the jmx tool log: %s" % lines

        for line in lines:
            if "time" in line:
                object_attribute_names = line.strip()[1:-1].split("\",\"")[1:]
                continue
            stats = [float(field) for field in line.split(',')]
            time_sec = int(stats[0]/1000)
            self.jmx_stats[idx-1][time_sec] = {name: stats[i+1] for i, name in enumerate(object_attribute_names)}

        # do not calculate average and maximum of jmx stats until we have read output from all nodes
        # If the service is multithreaded, this means that the results will be aggregated only when the last
        # service finishes
        if any(not time_to_stats for time_to_stats in self.jmx_stats):
            return

        start_time_sec = min([min(time_to_stats.keys()) for time_to_stats in self.jmx_stats])
        end_time_sec = max([max(time_to_stats.keys()) for time_to_stats in self.jmx_stats])

        for name in object_attribute_names:
            aggregates_per_time = []
            for time_sec in range(start_time_sec, end_time_sec + 1):
                # assume that value is 0 if it is not read by jmx tool at the given time. This is appropriate for metrics such as bandwidth
                values_per_node = [time_to_stats.get(time_sec, {}).get(name, 0) for time_to_stats in self.jmx_stats]
                # assume that value is aggregated across nodes by sum. This is appropriate for metrics such as bandwidth
                aggregates_per_time.append(sum(values_per_node))
            self.average_jmx_value[name] = sum(aggregates_per_time) / len(aggregates_per_time)
            self.maximum_jmx_value[name] = max(aggregates_per_time)

    def read_jmx_output_all_nodes(self):
        for node in self.nodes:
            self.read_jmx_output(self.idx(node), node)

    def jmxtool_version(self, node):
        # To correctly wait for requested JMX metrics to be added we need the --wait option for JmxTool. This option was
        # not added until 0.11.0.1, so any earlier versions need to use JmxTool from a newer version.
        version = get_version(node)
        return version

    def jmx_class_name(self, version):
        if version <= V_3_4_0:
            return "kafka.tools.JmxTool"
        else:
            return "org.apache.kafka.tools.JmxTool"

class JmxTool(JmxMixin, KafkaPathResolverMixin):
    """
    Simple helper class for using the JmxTool directly instead of as a mix-in
    """
    def __init__(self, text_context, *args, **kwargs):
        JmxMixin.__init__(self, num_nodes=1, *args, **kwargs)
        self.context = text_context

    @property
    def logger(self):
        return self.context.logger
