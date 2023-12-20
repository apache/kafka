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


from ducktape.mark import matrix, parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.utils import is_version
from kafkatest.version import LATEST_0_8_2, LATEST_0_9, LATEST_0_10_0, LATEST_0_10_1, DEV_BRANCH, KafkaVersion


class TestVerifiableProducer(Test):
    """Sanity checks on verifiable producer service class."""
    def __init__(self, test_context):
        super(TestVerifiableProducer, self).__init__(test_context)

        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=1) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}})

        self.num_messages = 1000
        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(test_context, num_nodes=1, kafka=self.kafka, topic=self.topic,
                                           max_messages=self.num_messages, throughput=self.num_messages // 10)
    def setUp(self):
        if self.zk:
            self.zk.start()

    @cluster(num_nodes=3)
    @parametrize(producer_version=str(LATEST_0_8_2))
    @parametrize(producer_version=str(LATEST_0_9))
    @parametrize(producer_version=str(LATEST_0_10_0))
    @parametrize(producer_version=str(LATEST_0_10_1))
    @matrix(producer_version=[str(DEV_BRANCH)], acks=["0", "1", "-1"], enable_idempotence=[False])
    @matrix(producer_version=[str(DEV_BRANCH)], acks=["-1"], enable_idempotence=[True])
    @matrix(producer_version=[str(DEV_BRANCH)], security_protocol=['PLAINTEXT', 'SSL'], metadata_quorum=quorum.all)
    @cluster(num_nodes=4)
    @matrix(producer_version=[str(DEV_BRANCH)], security_protocol=['SASL_SSL'], sasl_mechanism=['PLAIN', 'GSSAPI'],
            metadata_quorum=quorum.all)
    def test_simple_run(self, producer_version, acks=None, enable_idempotence=False, security_protocol = 'PLAINTEXT',
                        sasl_mechanism='PLAIN', metadata_quorum=quorum.zk):
        """
        Test that we can start VerifiableProducer on the current branch snapshot version or against the 0.8.2 jar, and
        verify that we can produce a small number of messages.
        """
        self.kafka.security_protocol = security_protocol
        self.kafka.client_sasl_mechanism = sasl_mechanism
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.interbroker_sasl_mechanism = sasl_mechanism
        if self.kafka.quorum_info.using_kraft:
            controller_quorum = self.kafka.controller_quorum
            controller_quorum.controller_security_protocol = security_protocol
            controller_quorum.controller_sasl_mechanism = sasl_mechanism
            controller_quorum.intercontroller_security_protocol = security_protocol
            controller_quorum.intercontroller_sasl_mechanism = sasl_mechanism
        self.kafka.start()

        node = self.producer.nodes[0]
        self.producer.enable_idempotence = enable_idempotence
        self.producer.acks = acks
        node.version = KafkaVersion(producer_version)
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=15,
             err_msg="Producer failed to start in a reasonable amount of time.")

        # using version.vstring (distutils.version.LooseVersion) is a tricky way of ensuring
        # that this check works with DEV_BRANCH
        # When running VerifiableProducer 0.8.X, both the current branch version and 0.8.X should show up because of the
        # way verifiable producer pulls in some development directories into its classpath
        #
        # If the test fails here because 'ps .. | grep' couldn't find the process it means
        # the login and grep that is_version() performs is slower than
        # the time it takes the producer to produce its messages.
        # Easy fix is to decrease throughput= above, the good fix is to make the producer
        # not terminate until explicitly killed in this case.
        if node.version <= LATEST_0_8_2:
            assert is_version(node, [node.version.vstring, LATEST_0_9.vstring], logger=self.logger)
        else:
            assert is_version(node, [node.version.vstring], logger=self.logger)

        self.producer.wait()
        num_produced = self.producer.num_acked
        assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)

    @cluster(num_nodes=4)
    @matrix(inter_broker_security_protocol=['PLAINTEXT', 'SSL'], metadata_quorum=[quorum.isolated_kraft])
    @matrix(inter_broker_security_protocol=['SASL_SSL'], inter_broker_sasl_mechanism=['PLAIN', 'GSSAPI'],
            metadata_quorum=[quorum.isolated_kraft])
    def test_multiple_kraft_security_protocols(
            self, inter_broker_security_protocol, inter_broker_sasl_mechanism='GSSAPI', metadata_quorum=quorum.isolated_kraft):
        """
        Test for remote KRaft cases that we can start VerifiableProducer on the current branch snapshot version, and
        verify that we can produce a small number of messages.  The inter-controller and broker-to-controller
        security protocols are defined to be different (which differs from the above test, where they were the same).
        """
        self.kafka.security_protocol = self.kafka.interbroker_security_protocol = inter_broker_security_protocol
        self.kafka.client_sasl_mechanism = self.kafka.interbroker_sasl_mechanism = inter_broker_sasl_mechanism
        controller_quorum = self.kafka.controller_quorum
        sasl_mechanism = 'PLAIN' if inter_broker_sasl_mechanism == 'GSSAPI' else 'GSSAPI'
        if inter_broker_security_protocol == 'PLAINTEXT':
            controller_security_protocol = 'SSL'
            intercontroller_security_protocol = 'SASL_SSL'
        elif inter_broker_security_protocol == 'SSL':
            controller_security_protocol = 'SASL_SSL'
            intercontroller_security_protocol = 'PLAINTEXT'
        else: # inter_broker_security_protocol == 'SASL_SSL'
            controller_security_protocol = 'PLAINTEXT'
            intercontroller_security_protocol = 'SSL'
        controller_quorum.controller_security_protocol = controller_security_protocol
        controller_quorum.controller_sasl_mechanism = sasl_mechanism
        controller_quorum.intercontroller_security_protocol = intercontroller_security_protocol
        controller_quorum.intercontroller_sasl_mechanism = sasl_mechanism
        self.kafka.start()

        node = self.producer.nodes[0]
        node.version = KafkaVersion(str(DEV_BRANCH))
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=15,
             err_msg="Producer failed to start in a reasonable amount of time.")

        # See above comment above regarding use of version.vstring (distutils.version.LooseVersion)
        assert is_version(node, [node.version.vstring], logger=self.logger)

        self.producer.wait()
        num_produced = self.producer.num_acked
        assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)

    @cluster(num_nodes=4)
    @parametrize(metadata_quorum=quorum.isolated_kraft)
    def test_multiple_kraft_sasl_mechanisms(self, metadata_quorum):
        """
        Test for remote KRaft cases that we can start VerifiableProducer on the current branch snapshot version, and
        verify that we can produce a small number of messages.  The inter-controller and broker-to-controller
        security protocols are both SASL_PLAINTEXT but the SASL mechanisms are different (we set
        GSSAPI for the inter-controller mechanism and PLAIN for the broker-to-controller mechanism).
        This test differs from the above tests -- he ones above used the same SASL mechanism for both paths.
        """
        self.kafka.security_protocol = self.kafka.interbroker_security_protocol = 'PLAINTEXT'
        controller_quorum = self.kafka.controller_quorum
        controller_quorum.controller_security_protocol = 'SASL_PLAINTEXT'
        controller_quorum.controller_sasl_mechanism = 'PLAIN'
        controller_quorum.intercontroller_security_protocol = 'SASL_PLAINTEXT'
        controller_quorum.intercontroller_sasl_mechanism = 'GSSAPI'
        self.kafka.start()

        node = self.producer.nodes[0]
        node.version = KafkaVersion(str(DEV_BRANCH))
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=15,
             err_msg="Producer failed to start in a reasonable amount of time.")

        # See above comment above regarding use of version.vstring (distutils.version.LooseVersion)
        assert is_version(node, [node.version.vstring], logger=self.logger)

        self.producer.wait()
        num_produced = self.producer.num_acked
        assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)

