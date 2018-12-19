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
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError

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

    @cluster(num_nodes=7)
    @parametrize(security_protocol='PLAINTEXT', interbroker_security_protocol='SSL')
    @parametrize(security_protocol='SSL', interbroker_security_protocol='PLAINTEXT')
    def test_client_ssl_endpoint_validation_failure(self, security_protocol, interbroker_security_protocol):
        """
        Test that invalid hostname in certificate results in connection failures.
        When security_protocol=SSL, client SSL handshakes are expected to fail due to hostname verification failure.
        When security_protocol=PLAINTEXT and interbroker_security_protocol=SSL, controller connections fail
        with hostname verification failure. Hence clients are expected to fail with LEADER_NOT_AVAILABLE.
        """

        SecurityConfig.ssl_stores = TestSslStores(self.test_context.local_scratch_dir,
                                                  valid_hostname=False)

        self.create_zookeeper()
        self.zk.start()

        self.create_kafka(security_protocol=security_protocol,
                          interbroker_security_protocol=interbroker_security_protocol)
        self.kafka.start()

        # We need more verbose logging to catch the expected errors
        self.create_and_start_clients(log_level="DEBUG")

        try:
            wait_until(lambda: self.producer.num_acked > 0, timeout_sec=5)

            # Fail quickly if messages are successfully acked
            raise RuntimeError("Messages published successfully but should not have!"
                               " Endpoint validation did not fail with invalid hostname")
        except TimeoutError:
            # expected
            pass

        error = 'SSLHandshakeException' if security_protocol == 'SSL' else 'LEADER_NOT_AVAILABLE'
        wait_until(lambda: self.producer_consumer_have_expected_error(error), timeout_sec=5)
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
