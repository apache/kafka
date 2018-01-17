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
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

/**
 * Tests command line SSL setup for reset tool.
 */
@Category({IntegrationTest.class})
public class ResetIntegrationWithSslTest extends AbstractResetIntegrationTest {

    static {
        try {
            try {
                sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");

                brokerProps.put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:0");
                brokerProps.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
                brokerProps.putAll(sslConfig);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ResetIntegrationWithSslTest(boolean sslEnabled) {
        super(sslEnabled);
    }

    @AfterClass
    public static void globalCleanup() {
        afterClassCleanup();
    }

    @Before
    public void before() throws Exception {
        prepareTest();
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }
}
