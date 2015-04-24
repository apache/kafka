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
import time


class ZookeeperService(Service):
    def __init__(self, service_context):
        """
        :type service_context ducktape.services.service.ServiceContext
        """
        super(ZookeeperService, self).__init__(service_context)
        self.logs = {"zk_log": "/mnt/zk.log"}

    def start(self):
        super(ZookeeperService, self).start()
        config = """
dataDir=/mnt/zookeeper
clientPort=2181
maxClientCnxns=0
initLimit=5
syncLimit=2
quorumListenOnAllIPs=true
"""
        for idx, node in enumerate(self.nodes, 1):
            template_params = { 'idx': idx, 'host': node.account.hostname }
            config += "server.%(idx)d=%(host)s:2888:3888\n" % template_params

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            node.account.ssh("mkdir -p /mnt/zookeeper")
            node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)
            node.account.create_file("/mnt/zookeeper.properties", config)
            node.account.ssh(
                "/opt/kafka/bin/zookeeper-server-start.sh /mnt/zookeeper.properties 1>> %(zk_log)s 2>> %(zk_log)s &"
                % self.logs)
            time.sleep(5)  # give it some time to start

    def stop_node(self, node, allow_fail=True):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.ssh("ps ax | grep -i 'zookeeper' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM",
                         allow_fail=allow_fail)

    def clean_node(self, node, allow_fail=True):
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log", allow_fail=allow_fail)

    def stop(self):
        """If the service left any running processes or data, clean them up."""
        super(ZookeeperService, self).stop()

        for idx, node in enumerate(self.nodes, 1):
            self.stop_node(node, allow_fail=False)
            self.clean_node(node)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        self.stop_node(node, allow_fail)
        self.clean_node(node, allow_fail)

    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])
