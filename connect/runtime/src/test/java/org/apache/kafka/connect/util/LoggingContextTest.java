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
package org.apache.kafka.connect.util;

import org.apache.kafka.connect.util.LoggingContext.Parameters;
import org.apache.kafka.connect.util.LoggingContext.Scope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoggingContextTest {

    private static final Logger log = LoggerFactory.getLogger(LoggingContextTest.class);

    private static final String CONNECTOR_NAME = "MyConnector";
    private static final ConnectorTaskId TASK_ID1 = new ConnectorTaskId(CONNECTOR_NAME, 1);
    private static final String EXTRA_KEY1 = "extra.key.1";
    private static final String EXTRA_VALUE1 = "value1";
    private static final String EXTRA_KEY2 = "extra.key.2";
    private static final String EXTRA_VALUE2 = "value2";

    private Map<String, String> mdc;

    @Before
    public void setup() {
        mdc = new HashMap<>();
        Map<String, String> existing = MDC.getCopyOfContextMap();
        if (existing != null) {
            mdc.putAll(existing);
        }
        MDC.put(EXTRA_KEY1, EXTRA_VALUE1);
        MDC.put(EXTRA_KEY2, EXTRA_VALUE2);
    }

    @After
    public void tearDown() {
        MDC.clear();
        MDC.setContextMap(mdc);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullConnectorNameForConnectorContext() {
        LoggingContext.forConnector(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTaskIdForTaskContext() {
        LoggingContext.forTask(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTaskIdForOffsetContext() {
        LoggingContext.forOffsets(null);
    }

    @Test
    public void shouldCreateConnectorLoggingContext() {
        assertMdcExtrasUntouched();
        assertMdc(null, null, null, null);

        LoggingContext.forConnector(CONNECTOR_NAME);
        assertMdc("MockSink", CONNECTOR_NAME, null, Scope.WORKER);
        log.info("Starting Connector");

        LoggingContext.clear();
        assertMdcExtrasUntouched();
        assertMdc(null, null, null, null);
    }

    @Test
    public void shouldCreateTaskLoggingContext() {
        assertMdcExtrasUntouched();
        LoggingContext.forTask(TASK_ID1);
        assertMdc("MockSink", TASK_ID1.connector(), TASK_ID1.task(), Scope.TASK);
        log.info("Running task");

        LoggingContext.clear();
        assertMdcExtrasUntouched();
        assertMdc(null, null, null, null);
    }

    @Test
    public void shouldCreateOffsetsLoggingContext() {
        assertMdcExtrasUntouched();
        LoggingContext.forOffsets(TASK_ID1);
        assertMdc("MockSink", TASK_ID1.connector(), TASK_ID1.task(), Scope.OFFSETS);
        log.info("Running task");

        LoggingContext.clear();
        assertMdcExtrasUntouched();
        assertMdc(null, null, null, null);
    }

    protected void assertMdc(String connectorShortClassName, String connectorName, Integer taskId, Scope scope) {
        assertEquals(connectorName, MDC.get(Parameters.CONNECTOR_NAME));
        assertEquals(taskId == null ? null : Integer.toString(taskId), MDC.get(Parameters.CONNECTOR_TASK));
        assertEquals(scope == null ? null : scope.value(), MDC.get(Parameters.CONNECTOR_SCOPE));
    }

    protected void assertMdcExtrasUntouched() {
        assertEquals(EXTRA_VALUE1, MDC.get(EXTRA_KEY1));
        assertEquals(EXTRA_VALUE2, MDC.get(EXTRA_KEY2));
    }
}