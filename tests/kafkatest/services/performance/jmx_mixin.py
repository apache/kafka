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

class JmxMixin(object):

    def __init__(self, num_nodes, jmx_object_name=None, jmx_attributes=None):
        self.jmx_object_name = jmx_object_name
        self.jmx_attributes = jmx_attributes
        self.jmx_port = 9192

        self.started = [False] * num_nodes
        self.jmx_stats = [{} for x in range(num_nodes)]
        self.maximum_jmx_value = {}  # map from object_attribute_name to maximum value observed over time
        self.average_jmx_value = {}  # map from object_attribute_name to average value observed over time

    def clean_node(self, node):
        node.account.kill_process("jmx", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/jmx_tool.log", allow_fail=False)

    def maybe_start_jmx_tool(self, idx, node):
        if self.started[idx-1] == True or self.jmx_object_name == None:
            return
        self.started[idx-1] = True

        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool " \
              "--reporting-interval 1000 --jmx-url service:jmx:rmi:///jndi/rmi://127.0.0.1:%d/jmxrmi" % self.jmx_port
        if self.jmx_object_name != None:
            cmd += " --object-name %s" % self.jmx_object_name
        if self.jmx_attributes != None:
            cmd += " --attributes %s" % self.jmx_attributes
        cmd += " > /mnt/jmx_tool.log &"

        self.logger.debug("Start JmxTool %d command: %s", idx, cmd)
        node.account.ssh(cmd, allow_fail=False)

    def read_jmx_output(self, idx, node):
        if self.started[idx-1] == False:
            return

        cmd = "cat /mnt/jmx_tool.log"
        self.logger.debug("Read jmx output %d command: %s", idx, cmd)

        object_attribute_names = []

        for line in node.account.ssh_capture(cmd, allow_fail=False):
            if "time" in line:
                object_attribute_names = line.strip()[1:-1].split("\",\"")[1:]
                continue
            stats = [float(field) for field in line.split(',')]
            time_sec = int(stats[0]/1000)
            self.jmx_stats[idx-1][time_sec] = {name : stats[i+1] for i, name in enumerate(object_attribute_names)}

        # do not calculate average and maximum of jmx stats until we have read output from all nodes
        if any(len(dict)==0 for dict in self.jmx_stats):
            return

        start_time_sec = min([min(dict.keys()) for dict in self.jmx_stats])
        end_time_sec = max([max(dict.keys()) for dict in self.jmx_stats])

        for name in object_attribute_names:
            aggregates_per_time = []
            for time_sec in xrange(start_time_sec, end_time_sec+1):
                # assume that value is 0 if it is not read by jmx tool at the given time. This is appropriate for metrics such as bandwidth
                values_per_node = [dict.get(time_sec, {}).get(name, 0) for dict in self.jmx_stats]
                # assume that value is aggregated across nodes by sum. This is appropriate for metrics such as bandwidth
                aggregates_per_time.append(sum(values_per_node))
            self.average_jmx_value[name] = sum(aggregates_per_time)/len(aggregates_per_time)
            self.maximum_jmx_value[name] = max(aggregates_per_time)