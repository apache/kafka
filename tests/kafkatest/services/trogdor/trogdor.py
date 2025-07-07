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

import json
import os.path
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka.util import get_log4j_config_param, get_log4j_config, \
    get_log4j_config_for_trogdor_coordinator, get_log4j_config_for_trogdor_agent


class TrogdorService(KafkaPathResolverMixin, Service):
    """
    A ducktape service for running the trogdor fault injection daemons.

    Attributes:
        PERSISTENT_ROOT                 The root filesystem path to store service files under.
        COORDINATOR_STDOUT_STDERR       The path where we store the coordinator's stdout/stderr output.
        AGENT_STDOUT_STDERR             The path where we store the agents's stdout/stderr output.
        COORDINATOR_LOG                 The path where we store the coordinator's log4j output.
        AGENT_LOG                       The path where we store the agent's log4j output.
        AGENT_LOG4J_PROPERTIES          The path to the agent log4j.properties file for log config.
        COORDINATOR_LOG4J_PROPERTIES    The path to the coordinator log4j.properties file for log config.
        CONFIG_PATH                     The path to the trogdor configuration file.
        DEFAULT_AGENT_PORT              The default port to use for trogdor_agent daemons.
        DEFAULT_COORDINATOR_PORT        The default port to use for trogdor_coordinator daemons.
        REQUEST_TIMEOUT                 The request timeout in seconds to use for REST requests.
        REQUEST_HEADERS                 The request headers to use when communicating with trogdor.
    """

    PERSISTENT_ROOT="/mnt/trogdor"
    COORDINATOR_STDOUT_STDERR = os.path.join(PERSISTENT_ROOT, "trogdor-coordinator-stdout-stderr.log")
    AGENT_STDOUT_STDERR = os.path.join(PERSISTENT_ROOT, "trogdor-agent-stdout-stderr.log")
    COORDINATOR_LOG = os.path.join(PERSISTENT_ROOT, "trogdor-coordinator.log")
    AGENT_LOG = os.path.join(PERSISTENT_ROOT, "trogdor-agent.log")
    CONFIG_PATH = os.path.join(PERSISTENT_ROOT, "trogdor.conf")
    DEFAULT_AGENT_PORT=8888
    DEFAULT_COORDINATOR_PORT=8889
    REQUEST_TIMEOUT=5
    REQUEST_HEADERS = {"Content-type": "application/json"}

    logs = {
        "trogdor_coordinator_stdout_stderr": {
            "path": COORDINATOR_STDOUT_STDERR,
            "collect_default": True},
        "trogdor_agent_stdout_stderr": {
            "path": AGENT_STDOUT_STDERR,
            "collect_default": True},
        "trogdor_coordinator_log": {
            "path": COORDINATOR_LOG,
            "collect_default": True},
        "trogdor_agent_log": {
            "path": AGENT_LOG,
            "collect_default": True},
    }


    def __init__(self, context, agent_nodes=None, client_services=None,
                 agent_port=DEFAULT_AGENT_PORT, coordinator_port=DEFAULT_COORDINATOR_PORT):
        """
        Create a Trogdor service.

        :param context:             The test context.
        :param agent_nodes:         The nodes to run the agents on.
        :param client_services:     Services whose nodes we should run agents on.
        :param agent_port:          The port to use for the trogdor_agent daemons.
        :param coordinator_port:    The port to use for the trogdor_coordinator daemons.
        """
        Service.__init__(self, context, num_nodes=1)
        self.coordinator_node = self.nodes[0]
        if client_services is not None:
            for client_service in client_services:
                for node in client_service.nodes:
                    self.nodes.append(node)
        if agent_nodes is not None:
            for agent_node in agent_nodes:
                self.nodes.append(agent_node)
        if (len(self.nodes) == 1):
            raise RuntimeError("You must supply at least one agent node to run the service on.")
        self.agent_port = agent_port
        self.coordinator_port = coordinator_port

    def free(self):
        # We only want to deallocate the coordinator node, not the agent nodes.  So we
        # change self.nodes to include only the coordinator node, and then invoke
        # the base class' free method.
        if self.coordinator_node is not None:
            self.nodes = [self.coordinator_node]
            self.coordinator_node = None
            Service.free(self)

    def _create_config_dict(self):
        """
        Create a dictionary with the Trogdor configuration.

        :return:            The configuration dictionary.
        """
        dict_nodes = {}
        for node in self.nodes:
            dict_nodes[node.name] = {
                "hostname": node.account.ssh_hostname,
            }
            if node.name == self.coordinator_node.name:
                dict_nodes[node.name]["trogdor.coordinator.port"] = self.coordinator_port
            else:
                dict_nodes[node.name]["trogdor.agent.port"] = self.agent_port

        return {
            "platform": "org.apache.kafka.trogdor.basic.BasicPlatform",
            "nodes": dict_nodes,
        }

    def start_node(self, node):
        node.account.mkdirs(TrogdorService.PERSISTENT_ROOT)

        # Create the configuration file on the node.
        str = json.dumps(self._create_config_dict(), indent=2)
        self.logger.info("Creating configuration file %s with %s" % (TrogdorService.CONFIG_PATH, str))
        node.account.create_file(TrogdorService.CONFIG_PATH, str)

        if self.is_coordinator(node):
            self._start_coordinator_node(node)
        else:
            self._start_agent_node(node)

    def _start_coordinator_node(self, node):
        node.account.create_file(get_log4j_config_for_trogdor_coordinator(node),
                                 self.render(get_log4j_config(node),
                                             log_path=TrogdorService.COORDINATOR_LOG))
        self._start_trogdor_daemon("coordinator", TrogdorService.COORDINATOR_STDOUT_STDERR,
                                   get_log4j_config_for_trogdor_coordinator(node),
                                   TrogdorService.COORDINATOR_LOG, node)
        self.logger.info("Started trogdor coordinator on %s." % node.name)

    def _start_agent_node(self, node):
        node.account.create_file(get_log4j_config_for_trogdor_agent(node),
                                 self.render(get_log4j_config(node),
                                             log_path=TrogdorService.AGENT_LOG))
        self._start_trogdor_daemon("agent", TrogdorService.AGENT_STDOUT_STDERR,
                                   get_log4j_config_for_trogdor_agent(node),
                                   TrogdorService.AGENT_LOG, node)
        self.logger.info("Started trogdor agent on %s." % node.name)

    def _start_trogdor_daemon(self, daemon_name, stdout_stderr_capture_path,
                              log4j_properties_path, log_path, node):
        cmd = "export KAFKA_LOG4J_OPTS='%s%s'; " % (get_log4j_config_param(node), log4j_properties_path)
        cmd += "%s %s --%s.config %s --node-name %s 1>> %s 2>> %s &" % \
               (self.path.script("trogdor.sh", node),
                daemon_name,
                daemon_name,
                TrogdorService.CONFIG_PATH,
                node.name,
                stdout_stderr_capture_path,
                stdout_stderr_capture_path)
        node.account.ssh(cmd)
        with node.account.monitor_log(stdout_stderr_capture_path) as monitor:
            monitor.wait_until("Starting %s process." % daemon_name, timeout_sec=60, backoff_sec=.10,
                               err_msg=("%s on %s didn't finish startup" % (daemon_name, node.name)))

    def wait_node(self, node, timeout_sec=None):
        if self.is_coordinator(node):
            return not node.account.java_pids(self.coordinator_class_name())
        else:
            return not node.account.java_pids(self.agent_class_name())

    def stop_node(self, node):
        """Halt trogdor processes on this node."""
        if self.is_coordinator(node):
            node.account.kill_java_processes(self.coordinator_class_name())
        else:
            node.account.kill_java_processes(self.agent_class_name())

    def clean_node(self, node):
        """Clean up persistent state on this node - e.g. service logs, configuration files etc."""
        self.stop_node(node)
        node.account.ssh("rm -rf -- %s" % TrogdorService.PERSISTENT_ROOT)

    def _coordinator_url(self, path):
        return "http://%s:%d/coordinator/%s" % \
               (self.coordinator_node.account.ssh_hostname, self.coordinator_port, path)

    def request_session(self):
        """
        Creates a new request session which will retry for a while.
        """
        session = requests.Session()
        session.mount('http://',
                      HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.3)))
        return session

    def _coordinator_post(self, path, message):
        """
        Make a POST request to the Trogdor coordinator.

        :param path:            The URL path to use.
        :param message:         The message object to send.
        :return:                The response as an object.
        """
        url = self._coordinator_url(path)
        self.logger.info("POST %s %s" % (url, message))
        response = self.request_session().post(url, json=message,
                                               timeout=TrogdorService.REQUEST_TIMEOUT,
                                               headers=TrogdorService.REQUEST_HEADERS)
        response.raise_for_status()
        return response.json()

    def _coordinator_put(self, path, message):
        """
        Make a PUT request to the Trogdor coordinator.

        :param path:            The URL path to use.
        :param message:         The message object to send.
        :return:                The response as an object.
        """
        url = self._coordinator_url(path)
        self.logger.info("PUT %s %s" % (url, message))
        response = self.request_session().put(url, json=message,
                                              timeout=TrogdorService.REQUEST_TIMEOUT,
                                              headers=TrogdorService.REQUEST_HEADERS)
        response.raise_for_status()
        return response.json()

    def _coordinator_get(self, path, message):
        """
        Make a GET request to the Trogdor coordinator.

        :param path:            The URL path to use.
        :param message:         The message object to send.
        :return:                The response as an object.
        """
        url = self._coordinator_url(path)
        self.logger.info("GET %s %s" % (url, message))
        response = self.request_session().get(url, json=message,
                                              timeout=TrogdorService.REQUEST_TIMEOUT,
                                              headers=TrogdorService.REQUEST_HEADERS)
        response.raise_for_status()
        return response.json()

    def create_task(self, id, spec):
        """
        Create a new task.

        :param id:          The task id.
        :param spec:        The task spec.
        """
        self._coordinator_post("task/create", { "id": id, "spec": spec.message})
        return TrogdorTask(id, self)

    def stop_task(self, id):
        """
        Stop a task.

        :param id:          The task id.
        """
        self._coordinator_put("task/stop", { "id": id })

    def tasks(self):
        """
        Get the tasks which are on the coordinator.

        :returns:           A map of task id strings to task state objects.
                            Task state objects contain a 'spec' field with the spec
                            and a 'state' field with the state.
        """
        return self._coordinator_get("tasks", {})

    def is_coordinator(self, node):
        return node == self.coordinator_node

    def agent_class_name(self):
        return "org.apache.kafka.trogdor.agent.Agent"

    def coordinator_class_name(self):
        return "org.apache.kafka.trogdor.coordinator.Coordinator"

class TrogdorTask(object):
    PENDING_STATE = "PENDING"
    RUNNING_STATE = "RUNNING"
    STOPPING_STATE = "STOPPING"
    DONE_STATE = "DONE"

    def __init__(self, id, trogdor):
        self.id = id
        self.trogdor = trogdor

    def task_state_or_error(self):
        task_state = self.trogdor.tasks()["tasks"][self.id]
        if task_state is None:
            raise RuntimeError("Coordinator did not know about %s." % self.id)
        error = task_state.get("error")
        if error is None or error == "":
            return task_state["state"], None
        else:
            return None, error

    def done(self):
        """
        Check if this task is done.

        :raises RuntimeError:       If the task encountered an error.
        :returns:                   True if the task is in DONE_STATE;
                                    False if it is in a different state.
        """
        (task_state, error) = self.task_state_or_error()
        if task_state is not None:
            return task_state == TrogdorTask.DONE_STATE
        else:
            raise RuntimeError("Failed to gracefully stop %s: got task error: %s" % (self.id, error))

    def running(self):
        """
        Check if this task is running.

        :raises RuntimeError:       If the task encountered an error.
        :returns:                   True if the task is in RUNNING_STATE;
                                    False if it is in a different state.
        """
        (task_state, error) = self.task_state_or_error()
        if task_state is not None:
            return task_state == TrogdorTask.RUNNING_STATE
        else:
            raise RuntimeError("Failed to start %s: got task error: %s" % (self.id, error))

    def stop(self):
        """
        Stop this task.

        :raises RuntimeError:       If the task encountered an error.
        """
        if self.done():
            return
        self.trogdor.stop_task(self.id)

    def wait_for_done(self, timeout_sec=360):
        wait_until(lambda: self.done(),
                   timeout_sec=timeout_sec,
                   err_msg="%s failed to finish in the expected amount of time." % self.id)
