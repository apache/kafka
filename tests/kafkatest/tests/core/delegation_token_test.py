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

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import config_property, KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.delegation_tokens import DelegationTokens
from kafkatest.services.verifiable_producer import VerifiableProducer

from datetime import datetime
import time

"""
Basic tests to validate delegation token support
"""
class DelegationTokenTest(Test):
    def __init__(self, test_context):
        super(DelegationTokenTest, self).__init__(test_context)

        self.test_context = test_context
        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, zk_chroot="/kafka",
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}},
                                  server_prop_overides=[
                                      [config_property.DELEGATION_TOKEN_MAX_LIFETIME_MS, "604800000"],
                                      [config_property.DELEGATION_TOKEN_EXPIRY_TIME_MS, "86400000"],
                                      [config_property.DELEGATION_TOKEN_MASTER_KEY, "test12345"],
                                      [config_property.SASL_ENABLED_MECHANISMS, "GSSAPI,SCRAM-SHA-256"]
                                  ])
        self.jaas_deleg_conf_path = "/tmp/jaas_deleg.conf"
        self.jaas_deleg_conf = ""
        self.client_properties_content = """
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.kerberos.service.name=kafka
client.id=console-consumer
"""
        self.client_kafka_opts=' -Djava.security.auth.login.config=' + self.jaas_deleg_conf_path

        self.producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka, topic=self.topic, max_messages=1,
                                       throughput=1, kafka_opts_override=self.client_kafka_opts,
                                       client_prop_file_override=self.client_properties_content)

        self.consumer = ConsoleConsumer(self.test_context, num_nodes=1, kafka=self.kafka, topic=self.topic,
                                        kafka_opts_override=self.client_kafka_opts,
                                        client_prop_file_override=self.client_properties_content)

        self.kafka.security_protocol = 'SASL_PLAINTEXT'
        self.kafka.client_sasl_mechanism = 'GSSAPI,SCRAM-SHA-256'
        self.kafka.interbroker_sasl_mechanism = 'GSSAPI'


    def setUp(self):
        self.zk.start()

    def tearDown(self):
        self.producer.nodes[0].account.remove(self.jaas_deleg_conf_path)
        self.consumer.nodes[0].account.remove(self.jaas_deleg_conf_path)

    def generate_delegation_token(self):
        self.logger.debug("Request delegation token")
        self.delegation_tokens.generate_delegation_token()
        self.jaas_deleg_conf = self.delegation_tokens.create_jaas_conf_with_delegation_token()

    def expire_delegation_token(self):
        self.kafka.client_sasl_mechanism = 'GSSAPI,SCRAM-SHA-256'
        token_hmac = self.delegation_tokens.token_hmac()
        self.delegation_tokens.expire_delegation_token(token_hmac)


    def produce_with_delegation_token(self):
        self.producer.acked_values = []
        self.producer.nodes[0].account.create_file(self.jaas_deleg_conf_path, self.jaas_deleg_conf)
        self.logger.debug(self.jaas_deleg_conf)
        self.producer.start()

    def consume_with_delegation_token(self):
        self.logger.debug("Consume messages with delegation token")

        self.consumer.nodes[0].account.create_file(self.jaas_deleg_conf_path, self.jaas_deleg_conf)
        self.logger.debug(self.jaas_deleg_conf)
        self.consumer.consumer_timeout_ms = 5000

        self.consumer.start()
        self.consumer.wait()

    def get_datetime_ms(self, input_date):
        return int(time.mktime(datetime.strptime(input_date,"%Y-%m-%dT%H:%M").timetuple()) * 1000)

    def renew_delegation_token(self):
        dt = self.delegation_tokens.parse_delegation_token_out()
        orig_expiry_date_ms = self.get_datetime_ms(dt["expirydate"])
        new_expirydate_ms = orig_expiry_date_ms + 1000

        self.delegation_tokens.renew_delegation_token(dt["hmac"], new_expirydate_ms)

    def test_delegation_token_lifecycle(self):
        self.kafka.start()
        self.delegation_tokens = DelegationTokens(self.kafka, self.test_context)

        self.generate_delegation_token()
        self.renew_delegation_token()
        self.produce_with_delegation_token()
        wait_until(lambda: self.producer.num_acked > 0, timeout_sec=30,
                   err_msg="Expected producer to still be producing.")
        assert 1 == self.producer.num_acked, "number of acked messages: %d" % self.producer.num_acked

        self.consume_with_delegation_token()
        num_consumed = len(self.consumer.messages_consumed[1])
        assert 1 == num_consumed, "number of consumed messages: %d" % num_consumed

        self.expire_delegation_token()

        self.produce_with_delegation_token()
        assert 0 == self.producer.num_acked, "number of acked messages: %d" % self.producer.num_acked