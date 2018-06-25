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

from kafkatest.directory_layout.kafka_path import TOOLS_JAR_NAME, TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.version import DEV_BRANCH, LATEST_0_8_2
from ducktape.cluster.remoteaccount import RemoteCommandError

import importlib
import os
import subprocess
import signal


"""This module abstracts the implementation of a verifiable client, allowing
client developers to plug in their own client for all kafkatests that make
use of either the VerifiableConsumer or VerifiableProducer classes.

A verifiable client class must implement exec_cmd() and pids().

This file provides:
 * VerifiableClientMixin class: to be used for creating new verifiable client classes
 * VerifiableClientJava class: the default Java verifiable clients
 * VerifiableClientApp class: uses global configuration to specify
   the command to execute and optional "pids" command, deploy script, etc.
   Config syntax (pass as --global <json_or_jsonfile>):
      {"Verifiable(Producer|Consumer|Client)": {
       "class": "kafkatest.services.verifiable_client.VerifiableClientApp",
       "exec_cmd": "/vagrant/x/myclient --some --standard --args",
       "pids": "pgrep -f ...", // optional
       "deploy": "/vagrant/x/mydeploy.sh", // optional
       "kill_signal": 2 // optional clean_shutdown kill signal (SIGINT in this case)
      }}
 * VerifiableClientDummy class: testing dummy



==============================
Verifiable client requirements
==============================

There are currently two verifiable client specifications:
 * VerifiableConsumer
 * VerifiableProducer

Common requirements for both:
 * One-way communication (client -> tests) through new-line delimited
   JSON objects on stdout (details below).
 * Log/debug to stderr

Common communication for both:
 * `{ "name": "startup_complete" }` - Client succesfully started
 * `{ "name": "shutdown_complete" }` - Client succesfully terminated (after receiving SIGINT/SIGTERM)


==================
VerifiableConsumer
==================

Command line arguments:
 * `--group-id <group-id>`
 * `--topic <topic>`
 * `--broker-list <brokers>`
 * `--session-timeout <n>`
 * `--enable-autocommit`
 * `--max-messages <n>`
 * `--assignment-strategy <s>`
 * `--consumer.config <config-file>` - consumer config properties (typically empty)

Environment variables:
 * `LOG_DIR` - log output directory. Typically not needed if logs are written to stderr.
 * `KAFKA_OPTS` - Security config properties (Java client syntax)
 * `KAFKA_LOG4J_OPTS` - Java log4j options (can be ignored)

Client communication:
 * `{ "name": "offsets_committed",  "success": bool, "error": "<errstr>", "offsets": [ { "topic": "<t>", "partition": <p>, "offset": <o> } ] }` - offset commit results, should be emitted for each committed offset. Emit prior to partitions_revoked.
 * `{ "name": "records_consumed", "partitions": [ { "topic": "<t>", "partition": <p>,  "minOffset": <o>, "maxOffset": <o> } ], "count": <total_consumed> }` - per-partition delta stats from last records_consumed. Emit every 1000 messages, or 1s. Emit prior to partitions_assigned, partitions_revoked and offsets_committed.
 * `{ "name": "partitions_revoked", "partitions": [ { "topic": "<t>", "partition": <p> } ] }` - rebalance: revoked partitions
 * `{ "name": "partitions_assigned", "partitions": [ { "topic": "<t>", "partition": <p> } ] }` - rebalance: assigned partitions


==================
VerifiableProducer
==================

Command line arguments:
 * `--topic <topic>`
 * `--broker-list <brokers>`
 * `--max-messages <n>`
 * `--throughput <msgs/s>`
 * `--producer.config <config-file>` - producer config properties (typically empty)

Environment variables:
 * `LOG_DIR` - log output directory. Typically not needed if logs are written to stderr.
 * `KAFKA_OPTS` - Security config properties (Java client syntax)
 * `KAFKA_LOG4J_OPTS` - Java log4j options (can be ignored)

Client communication:
 * `{ "name": "producer_send_error", "message": "<error msg>", "topic": "<t>", "key": "<msg key>", "value": "<msg value>" }` - emit on produce error.
 * `{ "name": "producer_send_success", "topic": "<t>", "partition": <p>, "offset": <o>, "key": "<msg key>", "value": "<msg value>" }` - emit on produce success.



===========
Development
===========

**Logs:**
During development of kafkatest clients it is generally a good idea to
enable collection of the client's stdout and stderr logs for troubleshooting.
Do this by setting "collect_default" to True for verifiable_consumder_stdout
and .._stderr in verifiable_consumer.py and verifiable_producer.py


**Deployment:**
There's currently no automatic way of deploying 3rd party kafkatest clients
on the VM instance so this needs to be done (at least partially) manually for
now.

One way to do this is logging in to a worker (`vagrant ssh worker1`), downloading
and building the kafkatest client under /vagrant (which maps to the kafka root
directory on the host and is shared with all VM instances).
Also make sure to install any system-level dependencies on each instance.

Then use /vagrant/..../yourkafkatestclient as your run-time path since it will
now be available on all instances.

The VerifiableClientApp automates the per-worker deployment with the optional
"deploy": "/vagrant/../deploy_script.sh" globals configuration property, this
script will be called on the VM just prior to executing the client.
"""

def create_verifiable_client_implementation(context, parent):
    """Factory for generating a verifiable client implementation class instance

    :param parent: parent class instance, either VerifiableConsumer or VerifiableProducer

    This will first check for a fully qualified client implementation class name
    in context.globals as "Verifiable<type>" where <type> is "Producer" or "Consumer",
    followed by "VerifiableClient" (which should implement both).
    The global object layout is: {"class": "<full class name>", "..anything..": ..}.

    If present, construct a new instance, else defaults to VerifiableClientJava
    """

    # Default class
    obj = {"class": "kafkatest.services.verifiable_client.VerifiableClientJava"}

    parent_name = parent.__class__.__name__.rsplit('.', 1)[-1]
    for k in [parent_name, "VerifiableClient"]:
        if k in context.globals:
            obj = context.globals[k]
            break

    if "class" not in obj:
        raise SyntaxError('%s (or VerifiableClient) expected object format: {"class": "full.class.path", ..}' % parent_name)

    clname = obj["class"]
    # Using the fully qualified classname, import the implementation class
    if clname.find('.') == -1:
        raise SyntaxError("%s (or VerifiableClient) must specify full class path (including module)" % parent_name)

    (module_name, clname) = clname.rsplit('.', 1)
    cluster_mod = importlib.import_module(module_name)
    impl_class = getattr(cluster_mod, clname)
    return impl_class(parent, obj)



class VerifiableClientMixin (object):
    """
    Verifiable client mixin class
    """
    @property
    def impl (self):
        """
        :return: Return (and create if necessary) the Verifiable client implementation object.
        """
        # Add _impl attribute to parent Verifiable(Consumer|Producer) object.
        if not hasattr(self, "_impl"):
            setattr(self, "_impl", create_verifiable_client_implementation(self.context, self))
            if hasattr(self.context, "logger") and self.context.logger is not None:
                self.context.logger.debug("Using client implementation %s for %s" % (self._impl.__class__.__name__, self.__class__.__name__))
        return self._impl


    def exec_cmd (self, node):
        """
        :return: command string to execute client.
        Environment variables will be prepended and command line arguments
        appended to this string later by start_cmd().

        This method should also take care of deploying the client on the instance, if necessary.
        """
        raise NotImplementedError()

    def pids (self, node):
        """ :return: list of pids for this client instance on node """
        raise NotImplementedError()

    def kill_signal (self, clean_shutdown=True):
        """ :return: the kill signal to terminate the application. """
        if not clean_shutdown:
            return signal.SIGKILL

        return self.conf.get("kill_signal", signal.SIGTERM)


class VerifiableClientJava (VerifiableClientMixin):
    """
    Verifiable Consumer and Producer using the official Java client.
    """
    def __init__(self, parent, conf=None):
        """
        :param parent: The parent instance, either VerifiableConsumer or VerifiableProducer
        :param conf: Optional conf object (the --globals VerifiableX object)
        """
        super(VerifiableClientJava, self).__init__()
        self.parent = parent
        self.java_class_name = parent.java_class_name()
        self.conf = conf

    def exec_cmd (self, node):
        """ :return: command to execute to start instance
        Translates Verifiable* to the corresponding Java client class name """
        cmd = ""
        if self.java_class_name == 'VerifiableProducer' and node.version <= LATEST_0_8_2:
            # 0.8.2.X releases do not have VerifiableProducer.java, so cheat and add
            # the tools jar from trunk to the classpath
            tools_jar = self.parent.path.jar(TOOLS_JAR_NAME, DEV_BRANCH)
            tools_dependant_libs_jar = self.parent.path.jar(TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)
            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % tools_jar
            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % tools_dependant_libs_jar
            cmd += "export CLASSPATH; "
        cmd += self.parent.path.script("kafka-run-class.sh", node) + " org.apache.kafka.tools." + self.java_class_name
        return cmd

    def pids (self, node):
        """ :return: pid(s) for this client intstance on node """
        try:
            cmd = "jps | grep -i " + self.java_class_name + " | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []


class VerifiableClientDummy (VerifiableClientMixin):
    """
    Dummy class for testing the pluggable framework
    """
    def __init__(self, parent, conf=None):
        """
        :param parent: The parent instance, either VerifiableConsumer or VerifiableProducer
        :param conf: Optional conf object (the --globals VerifiableX object)
        """
        super(VerifiableClientDummy, self).__init__()
        self.parent = parent
        self.conf = conf

    def exec_cmd (self, node):
        """ :return: command to execute to start instance """
        return 'echo -e \'{"name": "shutdown_complete" }\n\' ; echo ARGS:'

    def pids (self, node):
        """ :return: pid(s) for this client intstance on node """
        return []


class VerifiableClientApp (VerifiableClientMixin):
    """
    VerifiableClient using --global settings for exec_cmd, pids and deploy.
    By using this a verifiable client application can be used through simple
    --globals configuration rather than implementing a Python class.
    """

    def __init__(self, parent, conf):
        """
        :param parent: The parent instance, either VerifiableConsumer or VerifiableProducer
        :param conf: Optional conf object (the --globals VerifiableX object)
        """
        super(VerifiableClientApp, self).__init__()
        self.parent = parent
        # "VerifiableConsumer" or "VerifiableProducer"
        self.name = self.parent.__class__.__name__
        self.conf = conf

        if "exec_cmd" not in self.conf:
            raise SyntaxError("%s requires \"exec_cmd\": .. to be set in --globals %s object" % \
                              (self.__class__.__name__, self.name))

    def exec_cmd (self, node):
        """ :return: command to execute to start instance """
        self.deploy(node)
        return self.conf["exec_cmd"]

    def pids (self, node):
        """ :return: pid(s) for this client intstance on node """

        cmd = self.conf.get("pids", "pgrep -f '" + self.conf["exec_cmd"] + "'")
        try:
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            self.parent.context.logger.info("%s pids are: %s" % (str(node.account), pid_arr))
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def deploy (self, node):
        """ Call deploy script specified by "deploy" --global key
            This optional script is run on the VM instance just prior to
            executing `exec_cmd` to deploy the kafkatest client.
            The script path must be as seen by the VM instance, e.g. /vagrant/.... """

        if "deploy" not in self.conf:
            return

        script_cmd = self.conf["deploy"]
        self.parent.context.logger.debug("Deploying %s: %s" % (self, script_cmd))
        r = node.account.ssh(script_cmd)
