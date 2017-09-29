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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Properties;

/**
 * Tests command line SSL setup for reset tool.
 */
@Category({IntegrationTest.class})
public class ResetIntegrationWithSslTest extends AbstractResetIntegrationTest {

    private static Map<String, Object> sslConfig;
    static {
        try {
            sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER;
    static {
        final Properties props = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        props.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
        props.put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:9092");
        props.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
        props.putAll(sslConfig);
        // we align time to seconds to get clean window boundaries and thus ensure the same result for each run
        // otherwise, input records could fall into different windows for different runs depending on the initial mock time
        final long alignedTime = (System.currentTimeMillis() / 1000) * 1000;
        CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, props, alignedTime);
        cluster = CLUSTER;
    }

    @AfterClass
    public static void globalCleanup() {
        afterClassGlobalCleanup();
    }

    @Before
    public void before() throws Exception {
        beforePrepareTest();
    }

    Properties getClientSslConfig() {
        final Properties props = new Properties();

        props.put("bootstrap.servers", CLUSTER.bootstrapServers());
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

        return props;
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }

}
