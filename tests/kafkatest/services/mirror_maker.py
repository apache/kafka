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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin

"""
0.8.2.1 MirrorMaker options

Option                                  Description
------                                  -----------
--abort.on.send.failure <Stop the       Configure the mirror maker to exit on
  entire mirror maker when a send         a failed send. (default: true)
  failure occurs>
--blacklist <Java regex (String)>       Blacklist of topics to mirror.
--consumer.config <config file>         Embedded consumer config for consuming
                                          from the source cluster.
--consumer.rebalance.listener <A        The consumer rebalance listener to use
  custom rebalance listener of type       for mirror maker consumer.
  ConsumerRebalanceListener>
--help                                  Print this message.
--message.handler <A custom message     Message handler which will process
  handler of type                         every record in-between consumer and
  MirrorMakerMessageHandler>              producer.
--message.handler.args <Arguments       Arguments used by custom rebalance
  passed to message handler               listener for mirror maker consumer
  constructor.>
--num.streams <Integer: Number of       Number of consumption streams.
  threads>                                (default: 1)
--offset.commit.interval.ms <Integer:   Offset commit interval in ms (default:
  offset commit interval in               60000)
  millisecond>
--producer.config <config file>         Embedded producer config.
--rebalance.listener.args <Arguments    Arguments used by custom rebalance
  passed to custom rebalance listener     listener for mirror maker consumer
  constructor as a string.>
--whitelist <Java regex (String)>       Whitelist of topics to mirror.
"""


class MirrorMaker(KafkaPathResolverMixin, Service):

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/mirror_maker"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "mirror_maker.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    PRODUCER_CONFIG = os.path.join(PERSISTENT_ROOT, "producer.properties")
    CONSUMER_CONFIG = os.path.join(PERSISTENT_ROOT, "consumer.properties")

    logs = {
        "mirror_maker_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, source, target, whitelist=None, blacklist=None, num_streams=1,
                 new_consumer=True, consumer_timeout_ms=None, offsets_storage="kafka",
                 offset_commit_interval_ms=60000, log_level="DEBUG", producer_interceptor_classes=None):
        """
        MirrorMaker mirrors messages from one or more source clusters to a single destination cluster.

        Args:
            context:                    standard context
            source:                     source Kafka cluster
            target:                     target Kafka cluster to which data will be mirrored
            whitelist:                  whitelist regex for topics to mirror
            blacklist:                  blacklist regex for topics not to mirror
            num_streams:                number of consumer threads to create; can be a single int, or a list with
                                            one value per node, allowing num_streams to be the same for each node,
                                            or configured independently per-node
            consumer_timeout_ms:        consumer stops if t > consumer_timeout_ms elapses between consecutive messages
            offsets_storage:            used for consumer offsets.storage property
            offset_commit_interval_ms:  how frequently the mirror maker consumer commits offsets
        """
        super(MirrorMaker, self).__init__(context, num_nodes=num_nodes)
        self.log_level = log_level
        self.new_consumer = new_consumer
        self.consumer_timeout_ms = consumer_timeout_ms
        self.num_streams = num_streams
        if not isinstance(num_streams, int):
            # if not an integer, num_streams should be configured per-node
            assert len(num_streams) == num_nodes
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.source = source
        self.target = target

        self.offsets_storage = offsets_storage.lower()
        if not (self.offsets_storage in ["kafka", "zookeeper"]):
            raise Exception("offsets_storage should be 'kafka' or 'zookeeper'. Instead found %s" % self.offsets_storage)

        self.offset_commit_interval_ms = offset_commit_interval_ms
        self.producer_interceptor_classes = producer_interceptor_classes
        self.external_jars = None

        # These properties are potentially used by third-party tests.
        self.source_auto_offset_reset = None
        self.partition_assignment_strategy = None

    def start_cmd(self, node):
        cmd = "export LOG_DIR=%s;" % MirrorMaker.LOG_DIR
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\";" % MirrorMaker.LOG4J_CONFIG
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        # add external dependencies, for instance for interceptors
        if self.external_jars is not None:
            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % self.external_jars
            cmd += "export CLASSPATH; "
        cmd += " %s %s" % (self.path.script("kafka-run-class.sh", node),
                           self.java_class_name())
        cmd += " --consumer.config %s" % MirrorMaker.CONSUMER_CONFIG
        cmd += " --producer.config %s" % MirrorMaker.PRODUCER_CONFIG
        cmd += " --offset.commit.interval.ms %s" % str(self.offset_commit_interval_ms)
        if isinstance(self.num_streams, int):
            cmd += " --num.streams %d" % self.num_streams
        else:
            # config num_streams separately on each node
            cmd += " --num.streams %d" % self.num_streams[self.idx(node) - 1]
        if self.whitelist is not None:
            cmd += " --whitelist=\"%s\"" % self.whitelist
        if self.blacklist is not None:
            cmd += " --blacklist=\"%s\"" % self.blacklist

        cmd += " 1>> %s 2>> %s &" % (MirrorMaker.LOG_FILE, MirrorMaker.LOG_FILE)
        return cmd

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % MirrorMaker.PERSISTENT_ROOT, allow_fail=False)
        node.account.ssh("mkdir -p %s" % MirrorMaker.LOG_DIR, allow_fail=False)

        self.security_config = self.source.security_config.client_config()
        self.security_config.setup_node(node)

        # Create, upload one consumer config file for source cluster
        consumer_props = self.render("mirror_maker_consumer.properties")
        consumer_props += str(self.security_config)

        node.account.create_file(MirrorMaker.CONSUMER_CONFIG, consumer_props)
        self.logger.info("Mirrormaker consumer props:\n" + consumer_props)

        # Create, upload producer properties file for target cluster
        producer_props = self.render('mirror_maker_producer.properties')
        producer_props += str(self.security_config)
        self.logger.info("Mirrormaker producer props:\n" + producer_props)
        node.account.create_file(MirrorMaker.PRODUCER_CONFIG, producer_props)


        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=MirrorMaker.LOG_FILE)
        node.account.create_file(MirrorMaker.LOG4J_CONFIG, log_config)

        # Run mirror maker
        cmd = self.start_cmd(node)
        self.logger.debug("Mirror maker command: %s", cmd)
        node.account.ssh(cmd, allow_fail=False)
        wait_until(lambda: self.alive(node), timeout_sec=30, backoff_sec=.5,
                   err_msg="Mirror maker took to long to start.")
        self.logger.debug("Mirror maker is alive")

    def stop_node(self, node, clean_shutdown=True):
        node.account.kill_java_processes(self.java_class_name(), allow_fail=True,
                                         clean_shutdown=clean_shutdown)
        wait_until(lambda: not self.alive(node), timeout_sec=30, backoff_sec=.5,
                   err_msg="Mirror maker took to long to stop.")

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False,
                                         allow_fail=True)
        node.account.ssh("rm -rf %s" % MirrorMaker.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def java_class_name(self):
        return "kafka.tools.MirrorMaker"
