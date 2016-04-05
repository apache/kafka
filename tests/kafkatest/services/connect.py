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
import signal, random, requests, os.path, json

class ConnectServiceBase(Service):
    """Base class for Kafka Connect services providing some common settings and functionality"""

    PERSISTENT_ROOT = "/mnt/connect"
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "connect.properties")
    # The log file contains normal log4j logs written using a file appender. stdout and stderr are handled separately
    # so they can be used for other output, e.g. verifiable source & sink.
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "connect.log")
    STDOUT_FILE = os.path.join(PERSISTENT_ROOT, "connect.stdout")
    STDERR_FILE = os.path.join(PERSISTENT_ROOT, "connect.stderr")
    LOG4J_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "connect-log4j.properties")
    PID_FILE = os.path.join(PERSISTENT_ROOT, "connect.pid")

    logs = {
        "connect_log": {
            "path": LOG_FILE,
            "collect_default": True},
        "connect_stdout": {
            "path": STDOUT_FILE,
            "collect_default": False},
        "connect_stderr": {
            "path": STDERR_FILE,
            "collect_default": True},
    }

    def __init__(self, context, num_nodes, kafka, files):
        super(ConnectServiceBase, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()
        self.files = files

    def pids(self, node):
        """Return process ids for Kafka Connect processes."""
        try:
            return [pid for pid in node.account.ssh_capture("cat " + self.PID_FILE, callback=int)]
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
        self.logger.info((clean_shutdown and "Cleanly" or "Forcibly") + " stopping Kafka Connect on " + str(node.account))
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=True)
        if clean_shutdown:
            for pid in pids:
                wait_until(lambda: not node.account.alive(pid), timeout_sec=60, err_msg="Kafka Connect process on " + str(node.account) + " took too long to exit")

        node.account.ssh("rm -f " + self.PID_FILE, allow_fail=False)

    def restart(self):
        # We don't want to do any clean up here, just restart the process.
        for node in self.nodes:
            self.logger.info("Restarting Kafka Connect on " + str(node.account))
            self.stop_node(node)
            self.start_node(node)

    def clean_node(self, node):
        node.account.kill_process("connect", clean_shutdown=False, allow_fail=True)
        self.security_config.clean_node(node)
        node.account.ssh("rm -rf " + " ".join([self.CONFIG_FILE, self.LOG4J_CONFIG_FILE, self.PID_FILE, self.LOG_FILE, self.STDOUT_FILE, self.STDERR_FILE] + self.config_filenames() + self.files), allow_fail=False)

    def config_filenames(self):
        return [os.path.join(self.PERSISTENT_ROOT, "connect-connector-" + str(idx) + ".properties") for idx, template in enumerate(self.connector_config_templates or [])]


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
        self.logger.debug("Kafka Connect REST request: %s %s %s %s", node.account.hostname, url, method, body)
        resp = meth(url, json=body)
        self.logger.debug("%s %s response: %d", url, method, resp.status_code)
        if resp.status_code > 400:
            raise ConnectRestError(resp.status_code, resp.text, resp.url)
        if resp.status_code == 204:
            return None
        else:
            return resp.json()


    def _base_url(self, node):
        return 'http://' + node.account.externally_routable_ip + ':' + '8083'

class ConnectStandaloneService(ConnectServiceBase):
    """Runs Kafka Connect in standalone mode."""

    def __init__(self, context, kafka, files):
        super(ConnectStandaloneService, self).__init__(context, 1, kafka, files)

    # For convenience since this service only makes sense with a single node
    @property
    def node(self):
        return self.nodes[0]

    def start_cmd(self, node, connector_configs):
        cmd = "( export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % self.LOG4J_CONFIG_FILE
        cmd += "export KAFKA_OPTS=%s; " % self.security_config.kafka_opts
        cmd += "/opt/%s/bin/connect-standalone.sh %s " % (kafka_dir(node), self.CONFIG_FILE)
        cmd += " ".join(connector_configs)
        cmd += " & echo $! >&3 ) 1>> %s 2>> %s 3> %s" % (self.STDOUT_FILE, self.STDERR_FILE, self.PID_FILE)
        return cmd

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)

        self.security_config.setup_node(node)
        node.account.create_file(self.CONFIG_FILE, self.config_template_func(node))
        node.account.create_file(self.LOG4J_CONFIG_FILE, self.render('connect_log4j.properties', log_file=self.LOG_FILE))
        remote_connector_configs = []
        for idx, template in enumerate(self.connector_config_templates):
            target_file = os.path.join(self.PERSISTENT_ROOT, "connect-connector-" + str(idx) + ".properties")
            node.account.create_file(target_file, template)
            remote_connector_configs.append(target_file)

        self.logger.info("Starting Kafka Connect standalone process on " + str(node.account))
        with node.account.monitor_log(self.LOG_FILE) as monitor:
            node.account.ssh(self.start_cmd(node, remote_connector_configs))
            monitor.wait_until('Kafka Connect started', timeout_sec=15, err_msg="Never saw message indicating Kafka Connect finished startup on " + str(node.account))

        if len(self.pids(node)) == 0:
            raise RuntimeError("No process ids recorded")


class ConnectDistributedService(ConnectServiceBase):
    """Runs Kafka Connect in distributed mode."""

    def __init__(self, context, num_nodes, kafka, files, offsets_topic="connect-offsets",
                 configs_topic="connect-configs", status_topic="connect-status"):
        super(ConnectDistributedService, self).__init__(context, num_nodes, kafka, files)
        self.offsets_topic = offsets_topic
        self.configs_topic = configs_topic
        self.status_topic = status_topic

    def start_cmd(self, node):
        cmd = "( export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % self.LOG4J_CONFIG_FILE
        cmd += "export KAFKA_OPTS=%s; " % self.security_config.kafka_opts
        cmd += "/opt/%s/bin/connect-distributed.sh %s " % (kafka_dir(node), self.CONFIG_FILE)
        cmd += " & echo $! >&3 ) 1>> %s 2>> %s 3> %s" % (self.STDOUT_FILE, self.STDERR_FILE, self.PID_FILE)
        return cmd

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)

        self.security_config.setup_node(node)
        node.account.create_file(self.CONFIG_FILE, self.config_template_func(node))
        node.account.create_file(self.LOG4J_CONFIG_FILE, self.render('connect_log4j.properties', log_file=self.LOG_FILE))
        if self.connector_config_templates:
            raise DucktapeError("Config files are not valid in distributed mode, submit connectors via the REST API")

        self.logger.info("Starting Kafka Connect distributed process on " + str(node.account))
        with node.account.monitor_log(self.LOG_FILE) as monitor:
            node.account.ssh(self.start_cmd(node))
            monitor.wait_until('Kafka Connect started', timeout_sec=15, err_msg="Never saw message indicating Kafka Connect finished startup on " + str(node.account))

        if len(self.pids(node)) == 0:
            raise RuntimeError("No process ids recorded")




class ConnectRestError(RuntimeError):
    def __init__(self, status, msg, url):
        self.status = status
        self.message = msg
        self.url = url

    def __unicode__(self):
        return "Kafka Connect REST call failed: returned " + self.status + " for " + self.url + ". Response: " + self.message



class VerifiableConnector(object):
    def messages(self):
        """
        Collect and parse the logs from Kafka Connect nodes. Return a list containing all parsed JSON messages generated by
        this source.
        """
        self.logger.info("Collecting messages from log of %s %s", type(self).__name__, self.name)
        records = []
        for node in self.cc.nodes:
            for line in node.account.ssh_capture('cat ' + self.cc.STDOUT_FILE):
                try:
                    data = json.loads(line)
                except ValueError:
                    self.logger.debug("Ignoring unparseable line: %s", line)
                    continue
                # Filter to only ones matching our name to support multiple verifiable producers
                if data['name'] != self.name: continue
                data['node'] = node
                records.append(data)
        return records

    def stop(self):
        self.logger.info("Destroying connector %s %s", type(self).__name__, self.name)
        self.cc.delete_connector(self.name)

class VerifiableSource(VerifiableConnector):
    """
    Helper class for running a verifiable source connector on a Kafka Connect cluster and analyzing the output.
    """

    def __init__(self, cc, name="verifiable-source", tasks=1, topic="verifiable", throughput=1000):
        self.cc = cc
        self.logger = self.cc.logger
        self.name = name
        self.tasks = tasks
        self.topic = topic
        self.throughput = throughput

    def start(self):
        self.logger.info("Creating connector VerifiableSourceConnector %s", self.name)
        self.cc.create_connector({
            'name': self.name,
            'connector.class': 'org.apache.kafka.connect.tools.VerifiableSourceConnector',
            'tasks.max': self.tasks,
            'topic': self.topic,
            'throughput': self.throughput
        })

class VerifiableSink(VerifiableConnector):
    """
    Helper class for running a verifiable sink connector on a Kafka Connect cluster and analyzing the output.
    """

    def __init__(self, cc, name="verifiable-sink", tasks=1, topics=["verifiable"]):
        self.cc = cc
        self.logger = self.cc.logger
        self.name = name
        self.tasks = tasks
        self.topics = topics

    def start(self):
        self.logger.info("Creating connector VerifiableSinkConnector %s", self.name)
        self.cc.create_connector({
            'name': self.name,
            'connector.class': 'org.apache.kafka.connect.tools.VerifiableSinkConnector',
            'tasks.max': self.tasks,
            'topics': ",".join(self.topics)
        })
