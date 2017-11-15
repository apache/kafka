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
import kafka.tools.StreamsResetter;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.fail;

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

    @Test
    public void testReprocessingFromLatestEarliestAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromLatestAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void testReprocessingFromOffsetAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingFromOffsetAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void testReprocessingByShiftPositiveAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingByShiftPositiveAfterResetWithoutIntermediateUserTopic();
    }

    @Test
    public void testReprocessingByShiftNegativeAfterResetWithoutIntermediateUserTopic() throws Exception {
        super.testReprocessingByShiftNegativeAfterResetWithoutIntermediateUserTopic();
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
    public void shouldAcceptValidDateFormats() throws ParseException {
        //check valid formats
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
    }

    @Test
    public void shouldThrowOnInvalidDateFormat() throws ParseException {
        //check some invalid formats
        try {
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"));
            fail("Call to getDateTime should fail");
        } catch (final Exception e) {
            e.printStackTrace();
        }

        try {
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"));
            fail("Call to getDateTime should fail");
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void invokeGetDateTimeMethod(final SimpleDateFormat format) throws ParseException {
        final Date checkpoint = new Date();
        final StreamsResetter streamsResetter = new StreamsResetter();
        final String formattedCheckpoint = format.format(checkpoint);
        streamsResetter.getDateTime(formattedCheckpoint);
    }
}
