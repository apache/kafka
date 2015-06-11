# Copyright 2015 Confluent Inc.
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

from ducktape.services.background_thread import BackgroundThreadService


def is_int(msg):
    """Default method used to check whether text pulled from console consumer is a message.

    return int or None
    """
    try:
        return int(msg)
    except:
        return None


"""
0.8.2.1 ConsoleConsumer options

The console consumer is a tool that reads data from Kafka and outputs it to standard output.
Option                                  Description
------                                  -----------
--blacklist <blacklist>                 Blacklist of topics to exclude from
                                          consumption.
--consumer.config <config file>         Consumer config properties file.
--csv-reporter-enabled                  If set, the CSV metrics reporter will
                                          be enabled
--delete-consumer-offsets               If specified, the consumer path in
                                          zookeeper is deleted when starting up
--formatter <class>                     The name of a class to use for
                                          formatting kafka messages for
                                          display. (default: kafka.tools.
                                          DefaultMessageFormatter)
--from-beginning                        If the consumer does not already have
                                          an established offset to consume
                                          from, start with the earliest
                                          message present in the log rather
                                          than the latest message.
--max-messages <Integer: num_messages>  The maximum number of messages to
                                          consume before exiting. If not set,
                                          consumption is continual.
--metrics-dir <metrics dictory>         If csv-reporter-enable is set, and
                                          this parameter isset, the csv
                                          metrics will be outputed here
--property <prop>
--skip-message-on-error                 If there is an error when processing a
                                          message, skip it instead of halt.
--topic <topic>                         The topic id to consume on.
--whitelist <whitelist>                 Whitelist of topics to include for
                                          consumption.
--zookeeper <urls>                      REQUIRED: The connection string for
                                          the zookeeper connection in the form
                                          host:port. Multiple URLS can be
                                          given to allow fail-over.
"""


class ConsoleConsumer(BackgroundThreadService):
    logs = {
        "consumer_log": {
            "path": "/mnt/consumer.log",
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, kafka, topic, message_validator=is_int, from_beginning=True, consumer_timeout_ms=None):
        """
        Args:
            context:                    standard context
            num_nodes:                  number of nodes to use (this should be 1)
            kafka:                      kafka service
            topic:                      consume from this topic
            message_validator:          function which returns message or None
            from_beginning:             consume from beginning if True, else from the end
            consumer_timeout_ms:        corresponds to consumer.timeout.ms. consumer process ends if time between
                                        successively consumed messages exceeds this timeout. Setting this and
                                        waiting for the consumer to stop is a pretty good way to consume all messages
                                        in a topic.
        """
        super(ConsoleConsumer, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.args = {
            'topic': topic,
        }

        self.consumer_timeout_ms = consumer_timeout_ms

        self.from_beginning = from_beginning
        self.message_validator = message_validator
        self.messages_consumed = {idx: [] for idx in range(1, num_nodes + 1)}

    @property
    def start_cmd(self):
        args = self.args.copy()
        args.update({'zk_connect': self.kafka.zk.connect_setting()})
        cmd = "/opt/kafka/bin/kafka-console-consumer.sh --topic %(topic)s --zookeeper %(zk_connect)s" \
              " --consumer.config /mnt/console_consumer.properties" % args

        if self.from_beginning:
            cmd += " --from-beginning"

        cmd += " 2>> /mnt/consumer.log | tee -a /mnt/consumer.log &"
        return cmd

    def _worker(self, idx, node):
        # form config file
        if self.consumer_timeout_ms is not None:
            prop_file = self.render('console_consumer.properties', consumer_timeout_ms=self.consumer_timeout_ms)
        else:
            prop_file = self.render('console_consumer.properties')
        node.account.create_file("/mnt/console_consumer.properties", prop_file)

        # Run and capture output
        cmd = self.start_cmd
        self.logger.debug("Console consumer %d command: %s", idx, cmd)
        for line in node.account.ssh_capture(cmd):
            msg = line.strip()
            msg = self.message_validator(msg)
            if msg is not None:
                self.logger.debug("consumed a message: " + str(msg))
                self.messages_consumed[idx].append(msg)

    def start_node(self, node):
        super(ConsoleConsumer, self).start_node(node)

    def stop_node(self, node):
        node.account.kill_process("java", allow_fail=True)

    def clean_node(self, node):
        node.account.ssh("rm -rf /mnt/console_consumer.properties /mnt/consumer.log", allow_fail=True)

