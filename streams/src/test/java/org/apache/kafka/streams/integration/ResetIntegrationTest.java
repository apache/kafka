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
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Properties;


/**
 * Tests local state store and global application cleanup.
 */
@Category({IntegrationTest.class})
public class ResetIntegrationTest extends AbstractResetIntegrationTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER;

    private static final String TEST_ID = "reset-integration-test";

    static {
        final Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    @Override
    Map<String, Object> getClientSslConfig() {
        return null;
    }

    @Before
    public void before() throws Exception {
        testId = TEST_ID;
        cluster = CLUSTER;
        prepareTest();
    }

    @After
    public void after() throws Exception {
        cleanupTest();
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() throws Exception {
        super.testReprocessingFromScratchAfterResetWithIntermediateUserTopic();
    }

    @Test
    public void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromFileAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingByDurationAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void shouldNotAllowToResetWhileStreamsRunning() throws Exception {
        super.shouldNotAllowToResetWhileStreamsIsRunning();
    }

    @Test
    public void shouldNotAllowToResetWhenInputTopicAbsent() throws Exception {
        super.shouldNotAllowToResetWhenInputTopicAbsent();
    }

    @Test
    public void shouldNotAllowToResetWhenIntermediateTopicAbsent() throws Exception {
        super.shouldNotAllowToResetWhenIntermediateTopicAbsent();
    }
}
