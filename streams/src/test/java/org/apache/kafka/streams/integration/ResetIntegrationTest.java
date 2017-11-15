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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

/**
 * Tests local state store and global application cleanup.
 */
@Category({IntegrationTest.class})
public class ResetIntegrationTest extends AbstractResetIntegrationTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER;
    static {
        final Properties props = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        props.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
        CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, props);
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

    @Test
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() throws Exception {
        super.testReprocessingFromScratchAfterResetWithIntermediateUserTopic();
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }
}
