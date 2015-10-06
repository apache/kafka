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
        self.maximum_jmx_value = None
        self.average_jmx_value = None

    def clean_node(self, node):
        node.account.kill_process("jmx", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/jmx_tool.log", allow_fail=False)

    def maybe_start_jmx_tool(self, idx, node):
        if self.started[idx-1] == True or self.jmx_object_name == None:
            return
        self.started[idx-1] = True

        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool " \
              "--reporting-interval 1000 --jmx-url service:jmx:rmi:///jndi/rmi://127.0.0.1:%d/jmxrmi " \
              "--object-name %s " % (self.jmx_port, self.jmx_object_name)
        if self.jmx_attributes != None:
            cmd += "--attributes %s " % self.jmx_attributes
        cmd += "> /mnt/jmx_tool.log &"

        self.logger.debug("Start JmxTool %d command: %s", idx, cmd)
        node.account.ssh(cmd, allow_fail=False)

    def read_jmx_output(self, idx, node):
        if self.started[idx-1] == False:
            return

        cmd = "grep -v time /mnt/jmx_tool.log"
        self.logger.debug("Read jmx output %d command: %s", idx, cmd)

        for line in node.account.ssh_capture(cmd, allow_fail=False):
            time_sec = int(line.split(',')[0])/1000
            value = float(line.split(',')[1])
            self.jmx_stats[idx-1][time_sec] = value

        if any(len(dict)==0 for dict in self.jmx_stats):
            return

        # since we have read jmx stats from all nodes, calculate average and maximum of jmx stats
        start_time_sec = min([min(dict.keys()) for dict in self.jmx_stats])
        end_time_sec = max([max(dict.keys()) for dict in self.jmx_stats])
        values = []

        for time_sec in xrange(start_time_sec, end_time_sec+1):
            collection = [dict.get(time_sec, 0) for dict in self.jmx_stats]
            values.append(sum(collection))

        self.average_jmx_value = sum(values)/len(values)
        self.maximum_jmx_value = max(values)