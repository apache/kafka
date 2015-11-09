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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError

from kafkatest.services.kafka.directory import kafka_dir
import signal, random, requests

class ConnectServiceBase(Service):
    """Base class for Kafka Connect services providing some common settings and functionality"""

    logs = {
        "kafka_log": {
            "path": "/mnt/connect.log",
            "collect_default": True},
    }

    def __init__(self, context, num_nodes, kafka, files):
        super(ConnectServiceBase, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.files = files

    def pids(self, node):
        """Return process ids for Kafka Connect processes."""
        try:
            return [pid for pid in node.account.ssh_capture("cat /mnt/connect.pid", callback=int)]
        except:
            return []

    def set_configs(self, config_template_func, connector_config_templates=None):
        """
        Set configurations for the worker and the connector to run on
        it. These are not provided in the constructor because the worker
        config generally needs access to ZK/Kafka services to
        create the configuration.
        """
        self.config_template_func = config_template_func
        self.connector_config_templates = connector_config_templates

    def stop_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=False)
        for pid in pids:
            wait_until(lambda: not node.account.alive(pid), timeout_sec=10, err_msg="Kafka Connect standalone process took too long to exit")

        node.account.ssh("rm -f /mnt/connect.pid", allow_fail=False)

    def restart(self):
        # We don't want to do any clean up here, just restart the process.
        for node in self.nodes:
            self.stop_node(node)
            self.start_node(node)

    def clean_node(self, node):
        if len(self.pids(node)) > 0:
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        for pid in self.pids(node):
            node.account.signal(pid, signal.SIGKILL, allow_fail=False)

        node.account.ssh("rm -rf /mnt/connect.pid /mnt/connect.log /mnt/connect.properties  " + " ".join(self.config_filenames() + self.files), allow_fail=False)

    def config_filenames(self):
        return ["/mnt/connect-connector-" + str(idx) + ".properties" for idx, template in enumerate(self.connector_config_templates or [])]


    def list_connectors(self, node=None):
        return self._rest('/connectors', node=node)

    def create_connector(self, config, node=None):
        create_request = {
            'name': config['name'],
            'config': config
        }
        return self._rest('/connectors', create_request, node=node, method="POST")

    def get_connector(self, name, node=None):
        return self._rest('/connectors/' + name, node=node)

    def get_connector_config(self, name, node=None):
        return self._rest('/connectors/' + name + '/config', node=node)

    def set_connector_config(self, name, config, node=None):
        return self._rest('/connectors/' + name + '/config', config, node=node, method="PUT")

    def get_connector_tasks(self, name, node=None):
        return self._rest('/connectors/' + name + '/tasks', node=node)

    def delete_connector(self, name, node=None):
        return self._rest('/connectors/' + name, node=node, method="DELETE")

    def _rest(self, path, body=None, node=None, method="GET"):
        if node is None:
            node = random.choice(self.nodes)

        meth = getattr(requests, method.lower())
        url = self._base_url(node) + path
        resp = meth(url, json=body)
        self.logger.debug("%s %s response: %d", url, method, resp.status_code)
        if resp.status_code > 400:
            raise ConnectRestError(resp.status_code, resp.text, resp.url)
        if resp.status_code == 204:
            return None
        else:
            return resp.json()


    def _base_url(self, node):
        return 'http://' + node.account.hostname + ':' + '8083'

class ConnectStandaloneService(ConnectServiceBase):
    """Runs Kafka Connect in standalone mode."""

    def __init__(self, context, kafka, files):
        super(ConnectStandaloneService, self).__init__(context, 1, kafka, files)

    # For convenience since this service only makes sense with a single node
    @property
    def node(self):
        return self.nodes[0]

    def start_node(self, node):
        node.account.create_file("/mnt/connect.properties", self.config_template_func(node))
        remote_connector_configs = []
        for idx, template in enumerate(self.connector_config_templates):
            target_file = "/mnt/connect-connector-" + str(idx) + ".properties"
            node.account.create_file(target_file, template)
            remote_connector_configs.append(target_file)

        self.logger.info("Starting Kafka Connect standalone process")
        with node.account.monitor_log("/mnt/connect.log") as monitor:
            node.account.ssh("/opt/%s/bin/connect-standalone.sh /mnt/connect.properties " % kafka_dir(node) +
                             " ".join(remote_connector_configs) +
                             " 1>> /mnt/connect.log 2>> /mnt/connect.log & echo $! > /mnt/connect.pid")
            monitor.wait_until('Kafka Connect started', timeout_sec=10, err_msg="Never saw message indicating Kafka Connect finished startup")

        if len(self.pids(node)) == 0:
            raise RuntimeError("No process ids recorded")


class ConnectDistributedService(ConnectServiceBase):
    """Runs Kafka Connect in distributed mode."""

    def __init__(self, context, num_nodes, kafka, files, offsets_topic="connect-offsets", configs_topic="connect-configs"):
        super(ConnectDistributedService, self).__init__(context, num_nodes, kafka, files)
        self.offsets_topic = offsets_topic
        self.configs_topic = configs_topic

    def start_node(self, node):
        node.account.create_file("/mnt/connect.properties", self.config_template_func(node))
        if self.connector_config_templates:
            raise DucktapeError("Config files are not valid in distributed mode, submit connectors via the REST API")

        self.logger.info("Starting Kafka Connect distributed process")
        with node.account.monitor_log("/mnt/connect.log") as monitor:
            cmd = "/opt/%s/bin/connect-distributed.sh /mnt/connect.properties " % kafka_dir(node)
            cmd += " 1>> /mnt/connect.log 2>> /mnt/connect.log & echo $! > /mnt/connect.pid"
            node.account.ssh(cmd)
            monitor.wait_until('Kafka Connect started', timeout_sec=10, err_msg="Never saw message indicating Kafka Connect finished startup")

        if len(self.pids(node)) == 0:
            raise RuntimeError("No process ids recorded")




class ConnectRestError(RuntimeError):
    def __init__(self, status, msg, url):
        self.status = status
        self.message = msg
        self.url = url

    def __unicode__(self):
        return "Kafka Connect REST call failed: returned " + self.status + " for " + self.url + ". Response: " + self.message