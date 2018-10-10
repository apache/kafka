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

import org.apache.kafka.connect.tools.MockSinkConnector;
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
import static org.junit.Assert.assertTrue;

public class LoggingContextTest {

    private static final Logger log = LoggerFactory.getLogger(LoggingContextTest.class);

    private static final String CONNECTOR_NAME = "MyConnector";
    private static final ConnectorTaskId TASK_ID1 = new ConnectorTaskId(CONNECTOR_NAME, 1);
    private static final ConnectorTaskId TASK_ID2 = new ConnectorTaskId(CONNECTOR_NAME, 2);
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

    @Test
    public void shouldCreateConnectorContext() {
        assertMdcExtrasUntouched();
        assertMdc(null, null, null, null);

        try (LoggingContext ctx1 = LoggingContext.forConnector(MockSinkConnector.class.getName(), CONNECTOR_NAME)) {
            assertActive(ctx1);
            assertConnectorShortClassName(ctx1, "MockSink");
            assertConnectorName(ctx1, CONNECTOR_NAME);
            assertTaskId(ctx1, null);
            assertScope(ctx1, Scope.WORKER);
            assertMdc("MockSink", CONNECTOR_NAME, null, Scope.WORKER);
            log.info("Starting Connector");

            try (LoggingContext ctx2 = LoggingContext.forTask(TASK_ID1)) {
                assertActive(ctx1);
                assertActive(ctx2);
                assertConnectorShortClassName(ctx2, null);
                assertConnectorName(ctx2, TASK_ID1.connector());
                assertTaskId(ctx2, TASK_ID1.task());
                assertScope(ctx2, Scope.TASK);
                assertMdc("MockSink", TASK_ID1.connector(), TASK_ID1.task(), Scope.TASK);
                log.info("Running task");

                try (LoggingContext ctx3 = LoggingContext.forScope(Scope.OFFSETS)) {
                    assertActive(ctx1);
                    assertActive(ctx2);
                    assertActive(ctx3);
                    assertConnectorShortClassName(ctx3, null);
                    assertConnectorName(ctx3, null);
                    assertTaskId(ctx3, null);
                    assertScope(ctx3, Scope.OFFSETS);
                    assertMdc("MockSink", TASK_ID1.connector(), TASK_ID1.task(), Scope.OFFSETS);
                    log.info("Processing offsets");
                }

                assertActive(ctx1);
                assertActive(ctx2);
                assertConnectorShortClassName(ctx2, null);
                assertConnectorName(ctx2, TASK_ID1.connector());
                assertTaskId(ctx2, TASK_ID1.task());
                assertScope(ctx2, Scope.TASK);
                assertMdc("MockSink", TASK_ID1.connector(), TASK_ID1.task(), Scope.TASK);
            }
            assertActive(ctx1);
            assertConnectorShortClassName(ctx1, "MockSink");
            assertConnectorName(ctx1, CONNECTOR_NAME);
            assertTaskId(ctx1, null);
            assertScope(ctx1, Scope.WORKER);
            assertMdc("MockSink", CONNECTOR_NAME, null, Scope.WORKER);
        }

        assertMdcExtrasUntouched();
        assertMdc(null, null, null, null);
    }

    @Test
    public void shouldComputeShortName() {
        assertEquals("MySink", LoggingContext.shortNameFor("org.apache.something.MySinkConnector"));
        assertEquals("MySink", LoggingContext.shortNameFor("org.apache.something.MySinkConnectorTask"));
        assertEquals("MySink", LoggingContext.shortNameFor("org.apache.something.MySinkTask"));
        assertEquals("MySink", LoggingContext.shortNameFor("MySinkConnector"));
        assertEquals("MySink", LoggingContext.shortNameFor("MySinkConnectorTask"));
        assertEquals("MySink", LoggingContext.shortNameFor("MySinkTask"));
        assertEquals("MySink", LoggingContext.shortNameFor("org.apache.something.Other$MySinkConnector"));
        assertEquals("MySink", LoggingContext.shortNameFor("org.apache.something.Other$MySinkConnectorTask"));
        assertEquals("MySink", LoggingContext.shortNameFor("org.apache.something.Other$MySinkTask"));
    }

    protected void assertConnectorShortClassName(LoggingContext logContext, String expectedShortClassName) {
        assertEquals(expectedShortClassName, logContext.context.get(Parameters.CONNECTOR_CLASS));
    }

    protected void assertConnectorName(LoggingContext logContext, String expected) {
        assertEquals(expected, logContext.context.get(Parameters.CONNECTOR_NAME));
    }

    protected void assertTaskId(LoggingContext logContext, Integer expected) {
        assertEquals(expected == null ? null : Integer.toString(expected), logContext.context.get(Parameters.CONNECTOR_TASK));
    }

    protected void assertScope(LoggingContext context, Scope expected) {
        assertEquals(expected == null ? null : expected.value(), context.context.get(Parameters.CONNECTOR_SCOPE));
    }

    protected void assertMdc(String connectorShortClassName, String connectorName, Integer taskId, Scope scope) {
        assertEquals(connectorShortClassName, MDC.get(Parameters.CONNECTOR_CLASS));
        assertEquals(connectorName, MDC.get(Parameters.CONNECTOR_NAME));
        assertEquals(taskId == null ? null : Integer.toString(taskId), MDC.get(Parameters.CONNECTOR_TASK));
        assertEquals(scope == null ? null : scope.value(), MDC.get(Parameters.CONNECTOR_SCOPE));
    }

    protected void assertMdcExtrasUntouched() {
        assertEquals(EXTRA_VALUE1, MDC.get(EXTRA_KEY1));
        assertEquals(EXTRA_VALUE2, MDC.get(EXTRA_KEY2));
    }

    protected void assertActive(LoggingContext context) {
        assertTrue(context.isActive());
    }
}