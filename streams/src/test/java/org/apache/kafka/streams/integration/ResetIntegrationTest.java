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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import kafka.tools.StreamsResetter;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Properties;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;


/**
 * Tests local state store and global application cleanup.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class ResetIntegrationTest extends AbstractResetIntegrationTest {

    CLUSTER = new EmbeddedKafkaCluster(1);

    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;
    private static final String APP_ID = "Integration-test";

    private static final String NON_EXISTING_TOPIC = "nonExistingTopic";

    private static int testNo = 1;

    @AfterClass
    public static void afterClass() {
        afterClassCleanup();
    }

    @Before
    public void before() throws Exception {
        prepareTest();
    }

    @After
    void after() throws Exception {
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

        final List<String> parameterList = new ArrayList<>(
                Arrays.asList("--application-id", APP_ID + testNo,
                        "--bootstrap-servers", CLUSTER.bootstrapServers(),
                        "--input-topics", NON_EXISTING_TOPIC));

        final String[] parameters = parameterList.toArray(new String[parameterList.size()]);
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    @Test
    public void shouldNotAllowToResetWhenIntermediateTopicAbsent() throws Exception {

        final List<String> parameterList = new ArrayList<>(
                Arrays.asList("--application-id", APP_ID + testNo,
                        "--bootstrap-servers", CLUSTER.bootstrapServers(),
                        "--intermediate-topics", NON_EXISTING_TOPIC));

        final String[] parameters = parameterList.toArray(new String[parameterList.size()]);
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

}
