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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError

from kafkatest.services.kafka import quorum
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.security.security_config import SslStores
from kafkatest.tests.end_to_end import EndToEndTest

class TestSslStores(SslStores):
    def __init__(self, local_scratch_dir, valid_hostname=True):
        super(TestSslStores, self).__init__(local_scratch_dir)
        self.valid_hostname = valid_hostname
        self.generate_ca()
        self.generate_truststore()

    def hostname(self, node):
        if self.valid_hostname:
            return super(TestSslStores, self).hostname(node)
        else:
            return "invalidhostname"

class SecurityTest(EndToEndTest):
    """
    These tests validate security features.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(SecurityTest, self).__init__(test_context=test_context)

    def producer_consumer_have_expected_error(self, error):
        try:
            for node in self.producer.nodes:
                node.account.ssh("grep %s %s" % (error, self.producer.LOG_FILE))
            for node in self.consumer.nodes:
                node.account.ssh("grep %s %s" % (error, self.consumer.LOG_FILE))
        except RemoteCommandError:
            return False

        return True

    @cluster(num_nodes=6)
    @matrix(
        security_protocol=['PLAINTEXT'],
        interbroker_security_protocol=['SSL'],
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        security_protocol=['PLAINTEXT'],
        interbroker_security_protocol=['SSL'],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    @matrix(
        security_protocol=['SSL'],
        interbroker_security_protocol=['PLAINTEXT'],
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        security_protocol=['SSL'],
        interbroker_security_protocol=['PLAINTEXT'],
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_client_ssl_endpoint_validation_failure(self, security_protocol, interbroker_security_protocol, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Test that invalid hostname in certificate results in connection failures.
        When security_protocol=SSL, client SSL handshakes are expected to fail due to hostname verification failure.
        When security_protocol=PLAINTEXT and interbroker_security_protocol=SSL, controller connections fail
        with hostname verification failure. Since metadata cannot be propagated in the cluster without a valid certificate,
        the broker's metadata caches will be empty. Hence we expect Metadata requests to fail with an INVALID_REPLICATION_FACTOR
        error since the broker will attempt to create the topic automatically as it does not exist in the metadata cache,
        and there will be no online brokers.
        """

        # Start Kafka with valid hostnames in the certs' SANs so that we can create the test topic via the admin client
        SecurityConfig.ssl_stores = TestSslStores(self.test_context.local_scratch_dir,
                                                  valid_hostname=True)

        self.create_zookeeper_if_necessary()
        if self.zk:
            self.zk.start()

        self.create_kafka(security_protocol=security_protocol,
                          interbroker_security_protocol=interbroker_security_protocol)
        if self.kafka.quorum_info.using_kraft and interbroker_security_protocol == 'SSL':
            # we don't want to interfere with communication to the controller quorum
            # (we separately test this below) so make sure it isn't using TLS
            # (it uses the inter-broker security information by default)
            controller_quorum = self.kafka.controller_quorum
            controller_quorum.controller_security_protocol = 'PLAINTEXT'
            controller_quorum.intercontroller_security_protocol = 'PLAINTEXT'
        self.kafka.start()

        # now set the certs to have invalid hostnames so we can run the actual test
        SecurityConfig.ssl_stores.valid_hostname = False
        self.kafka.restart_cluster()

        if self.kafka.quorum_info.using_kraft and security_protocol == 'PLAINTEXT':
            # the inter-broker security protocol using TLS with a hostname verification failure
            # doesn't impact a producer in case of a single broker with a KRaft Controller,
            # so confirm that this is in fact the observed behavior
            self.create_and_start_clients(log_level="INFO")
            self.run_validation()
        else:
            # We need more verbose logging to catch the expected errors
            self.create_and_start_clients(log_level="DEBUG")

            try:
                wait_until(lambda: self.producer.num_acked > 0, timeout_sec=30)

                # Fail quickly if messages are successfully acked
                raise RuntimeError("Messages published successfully but should not have!"
                                   " Endpoint validation did not fail with invalid hostname")
            except TimeoutError:
                # expected
                pass

            error = 'SSLHandshakeException' if security_protocol == 'SSL' else 'INVALID_REPLICATION_FACTOR'
            wait_until(lambda: self.producer_consumer_have_expected_error(error), timeout_sec=30)
            self.producer.stop()
            self.consumer.stop()

            SecurityConfig.ssl_stores.valid_hostname = True
            self.kafka.restart_cluster()
            self.create_and_start_clients(log_level="INFO")
            self.run_validation()

    def create_and_start_clients(self, log_level):
        self.create_producer(log_level=log_level)
        self.producer.start()

        self.create_consumer(log_level=log_level)
        self.consumer.start()

    @cluster(num_nodes=2)
    @matrix(
        metadata_quorum=[quorum.zk],
        use_new_coordinator=[False]
    )
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        use_new_coordinator=[True, False]
    )
    def test_quorum_ssl_endpoint_validation_failure(self, metadata_quorum=quorum.zk, use_new_coordinator=False):
        """
        Test that invalid hostname in ZooKeeper or KRaft Controller results in broker inability to start.
        """
        # Start ZooKeeper/KRaft Controller with valid hostnames in the certs' SANs
        # so that we can start Kafka
        SecurityConfig.ssl_stores = TestSslStores(self.test_context.local_scratch_dir,
                                                  valid_hostname=True)

        self.create_zookeeper_if_necessary(num_nodes=1,
                                           zk_client_port = False,
                                           zk_client_secure_port = True,
                                           zk_tls_encrypt_only = True,
                                           )
        if self.zk:
            self.zk.start()

        self.create_kafka(num_nodes=1,
                          interbroker_security_protocol='SSL', # also sets the broker-to-kraft-controller security protocol for the KRaft case
                          zk_client_secure=True, # ignored if we aren't using ZooKeeper
                          )
        self.kafka.start()

        # now stop the Kafka broker
        # and set the cert for ZooKeeper/KRaft Controller to have an invalid hostname
        # so we can restart Kafka and ensure it is unable to start
        self.kafka.stop_node(self.kafka.nodes[0])

        SecurityConfig.ssl_stores.valid_hostname = False
        if quorum.for_test(self.test_context) == quorum.zk:
            self.kafka.zk.restart_cluster()
        else:
            self.kafka.isolated_controller_quorum.restart_cluster()

        try:
            self.kafka.start_node(self.kafka.nodes[0], timeout_sec=30)
            raise RuntimeError("Kafka restarted successfully but should not have!"
                               " Endpoint validation did not fail with invalid hostname")
        except TimeoutError:
            # expected
            pass
