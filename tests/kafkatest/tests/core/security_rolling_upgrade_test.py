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
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.utils import is_int
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.mark import matrix
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from kafkatest.services.security.kafka_acls import ACLs
import time


class TestSecurityRollingUpgrade(ProduceConsumeValidateTest):
    """Tests a rolling upgrade from PLAINTEXT to a secured cluster
    """

    def __init__(self, test_context):
        super(TestSecurityRollingUpgrade, self).__init__(test_context=test_context)

    def setUp(self):
        self.acls = ACLs(self.test_context)
        self.topic = "test_topic"
        self.group = "group"
        self.producer_throughput = 100
        self.num_producers = 1
        self.num_consumers = 1
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=None, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            'configs': {"min.insync.replicas": 2}}},
            controller_num_nodes_override=1)

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, self.num_producers, self.kafka, self.topic,
            throughput=self.producer_throughput)

        self.consumer = ConsoleConsumer(
            self.test_context, self.num_consumers, self.kafka, self.topic,
            consumer_timeout_ms=60000, message_validator=is_int)

        self.consumer.group_id = "group"

    def bounce(self):
        self.kafka.start_minikdc_if_necessary()
        self.kafka.restart_cluster(after_each_broker_restart = lambda: time.sleep(10))

    def open_secured_port(self, client_protocol):
        self.kafka.security_protocol = client_protocol
        self.kafka.open_port(client_protocol)
        self.kafka.start_minikdc_if_necessary()
        self.bounce()

    def add_sasl_mechanism(self, new_client_sasl_mechanism):
        self.kafka.client_sasl_mechanism = new_client_sasl_mechanism
        self.bounce()

    def add_separate_broker_listener(self, broker_security_protocol, broker_sasl_mechanism):
        # Enable the new internal listener on all brokers first
        self.kafka.open_port(self.kafka.INTERBROKER_LISTENER_NAME)
        self.kafka.port_mappings[self.kafka.INTERBROKER_LISTENER_NAME].security_protocol = broker_security_protocol
        self.kafka.port_mappings[self.kafka.INTERBROKER_LISTENER_NAME].sasl_mechanism = broker_sasl_mechanism
        self.bounce()

        # Update inter-broker listener after all brokers have been updated to enable the new listener
        self.kafka.setup_interbroker_listener(broker_security_protocol, True)
        self.kafka.interbroker_sasl_mechanism = broker_sasl_mechanism
        self.bounce()

    def remove_separate_broker_listener(self, client_security_protocol):
        # This must be done in two phases: keep listening on the INTERNAL listener while rolling once to switch
        # the inter-broker security listener, then roll again to remove the INTERNAL listener.
        orig_inter_broker_security_protocol = self.kafka.interbroker_security_protocol
        self.kafka.setup_interbroker_listener(client_security_protocol, False)  # this closes the INTERNAL listener
        # Re-open the INTERNAL listener
        self.kafka.open_port(KafkaService.INTERBROKER_LISTENER_NAME)
        self.kafka.port_mappings[KafkaService.INTERBROKER_LISTENER_NAME].security_protocol = orig_inter_broker_security_protocol
        self.kafka.port_mappings[KafkaService.INTERBROKER_LISTENER_NAME].sasl_mechanism = self.kafka.interbroker_sasl_mechanism
        self.bounce()
        # Close the INTERNAL listener for good and bounce again to fully migrate to <client_security_protocol>
        self.kafka.close_port(KafkaService.INTERBROKER_LISTENER_NAME)
        self.bounce()

    @cluster(num_nodes=8)
    @matrix(client_protocol=[SecurityConfig.SSL], metadata_quorum=[quorum.isolated_kraft])
    @cluster(num_nodes=9)
    @matrix(client_protocol=[SecurityConfig.SASL_PLAINTEXT, SecurityConfig.SASL_SSL], metadata_quorum=[quorum.isolated_kraft])
    def test_rolling_upgrade_phase_one(self, client_protocol, metadata_quorum):
        """
        Start with a PLAINTEXT cluster, open a SECURED port, via a rolling upgrade, ensuring we could produce
        and consume throughout over PLAINTEXT. Finally check we can produce and consume the new secured port.
        """
        self.kafka.setup_interbroker_listener(SecurityConfig.PLAINTEXT)
        self.kafka.security_protocol = SecurityConfig.PLAINTEXT
        self.kafka.start()

        # Create PLAINTEXT producer and consumer
        self.create_producer_and_consumer()

        # Rolling upgrade, opening a secure protocol, ensuring the Plaintext producer/consumer continues to run
        self.run_produce_consume_validate(self.open_secured_port, client_protocol)

        # Now we can produce and consume via the secured port
        self.kafka.security_protocol = client_protocol
        self.create_producer_and_consumer()
        self.run_produce_consume_validate(lambda: time.sleep(1))

    @cluster(num_nodes=9)
    @matrix(new_client_sasl_mechanism=[SecurityConfig.SASL_MECHANISM_PLAIN], metadata_quorum=[quorum.isolated_kraft])
    def test_rolling_upgrade_sasl_mechanism_phase_one(self, new_client_sasl_mechanism, metadata_quorum):
        """
        Start with a SASL/GSSAPI cluster, add new SASL mechanism, via a rolling upgrade, ensuring we could produce
        and consume throughout over SASL/GSSAPI. Finally check we can produce and consume using new mechanism.
        """
        self.kafka.setup_interbroker_listener(SecurityConfig.SASL_SSL, use_separate_listener=False)
        self.kafka.security_protocol = SecurityConfig.SASL_SSL
        self.kafka.client_sasl_mechanism = SecurityConfig.SASL_MECHANISM_GSSAPI
        self.kafka.interbroker_sasl_mechanism = SecurityConfig.SASL_MECHANISM_GSSAPI
        self.kafka.start()

        # Create SASL/GSSAPI producer and consumer
        self.create_producer_and_consumer()

        # Rolling upgrade, adding new SASL mechanism, ensuring the GSSAPI producer/consumer continues to run
        self.run_produce_consume_validate(self.add_sasl_mechanism, new_client_sasl_mechanism)

        # Now we can produce and consume using the new SASL mechanism
        self.kafka.client_sasl_mechanism = new_client_sasl_mechanism
        self.create_producer_and_consumer()
        self.run_produce_consume_validate(lambda: time.sleep(1))

    @cluster(num_nodes=9)
    @parametrize(metadata_quorum=quorum.isolated_kraft)
    def test_enable_separate_interbroker_listener(self, metadata_quorum):
        """
        Start with a cluster that has a single PLAINTEXT listener.
        Start producing/consuming on PLAINTEXT port.
        While doing that, do a rolling restart to enable separate secured interbroker port
        """
        self.kafka.security_protocol = SecurityConfig.PLAINTEXT
        self.kafka.setup_interbroker_listener(SecurityConfig.PLAINTEXT, use_separate_listener=False)

        self.kafka.start()

        self.create_producer_and_consumer()

        self.run_produce_consume_validate(self.add_separate_broker_listener, SecurityConfig.SASL_SSL,
                                          SecurityConfig.SASL_MECHANISM_PLAIN)

    @cluster(num_nodes=9)
    @parametrize(metadata_quorum=quorum.isolated_kraft)
    def test_disable_separate_interbroker_listener(self, metadata_quorum):
        """
        Start with a cluster that has two listeners, one on SSL (clients), another on SASL_SSL (broker-to-broker).
        Start producer and consumer on SSL listener.
        Close dedicated interbroker listener via rolling restart.
        Ensure we can produce and consume via SSL listener throughout.
        """
        client_protocol = SecurityConfig.SSL
        interbroker_security_protocol = SecurityConfig.SASL_SSL
        interbroker_sasl_mechanism = SecurityConfig.SASL_MECHANISM_GSSAPI

        self.kafka.security_protocol = client_protocol
        self.kafka.setup_interbroker_listener(interbroker_security_protocol, use_separate_listener=True)
        self.kafka.interbroker_sasl_mechanism = interbroker_sasl_mechanism

        self.kafka.start()
        # create producer and consumer via client security protocol
        self.create_producer_and_consumer()

        # run produce/consume/validate loop while disabling a separate interbroker listener via rolling restart
        self.run_produce_consume_validate(
            self.remove_separate_broker_listener, client_protocol)

