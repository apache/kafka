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
package org.apache.kafka.streams.integration;

import kafka.server.KafkaConfig$;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Tests command line SSL setup for reset tool.
 */
@Category({IntegrationTest.class})
public class ResetIntegrationWithSslTest extends AbstractResetIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER;

    private static final Map<String, Object> SSL_CONFIG;

    static {
        final Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);

        try {
            SSL_CONFIG = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");

            brokerProps.put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:0");
            brokerProps.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
            brokerProps.putAll(SSL_CONFIG);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Override
    Map<String, Object> getClientSslConfig() {
        return SSL_CONFIG;
    }

    @Before
    public void before() throws Exception {
        cluster = CLUSTER;
        prepareTest();
    }

    @After 
    public void after() throws Exception {
        cleanupTest();
    }

}
