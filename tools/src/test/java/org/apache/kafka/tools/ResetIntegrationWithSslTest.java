/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Tests command line SSL setup for reset tool.
 */
@Tag("integration")
public class ResetIntegrationWithSslTest extends AbstractResetIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER;

    private static final Map<String, Object> SSL_CONFIG;

    static {
        final Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, -1L);

        try {
            SSL_CONFIG = TestSslUtils.createSslConfig(false, true, ConnectionMode.SERVER, TestUtils.tempFile(), "testCert");
            brokerProps.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "EXTERNAL:SSL,CONTROLLER:SSL,INTERNAL:SSL");
            brokerProps.putAll(SSL_CONFIG);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Override
    Map<String, Object> getClientSslConfig() {
        return SSL_CONFIG;
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        cluster = CLUSTER;
        prepareTest(testInfo);
    }

    @AfterEach
    public void after() throws Exception {
        cleanupTest();
    }

}
