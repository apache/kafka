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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LoggingContextTest {

    private static final Logger log = LoggerFactory.getLogger(LoggingContextTest.class);

    private static final String CONNECTOR_NAME = "MyConnector";
    private static final ConnectorTaskId TASK_ID1 = new ConnectorTaskId(CONNECTOR_NAME, 1);
    private static final String EXTRA_KEY1 = "extra.key.1";
    private static final String EXTRA_VALUE1 = "value1";
    private static final String EXTRA_KEY2 = "extra.key.2";
    private static final String EXTRA_VALUE2 = "value2";
    private static final String EXTRA_KEY3 = "extra.key.3";
    private static final String EXTRA_VALUE3 = "value3";

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
    public void shouldNotAllowNullConnectorNameForConnectorContext() {
        assertThrows(NullPointerException.class, () -> LoggingContext.forConnector(null));
    }

    @Test
    public void shouldNotAllowNullTaskIdForTaskContext() {
        assertThrows(NullPointerException.class, () -> LoggingContext.forTask(null));
    }

    @Test
    public void shouldNotAllowNullTaskIdForOffsetContext() {
        assertThrows(NullPointerException.class, () -> LoggingContext.forOffsets(null));
    }

    @Test
    public void shouldCreateAndCloseLoggingContextEvenWithNullContextMap() {
        MDC.clear();
        assertMdc(null, null, null);
        try (LoggingContext loggingContext = LoggingContext.forConnector(CONNECTOR_NAME)) {
            assertMdc(CONNECTOR_NAME, null, Scope.WORKER);
            log.info("Starting Connector");
        }
        assertMdc(null, null, null);
    }

    @Test
    public void shouldCreateConnectorLoggingContext() {
        assertMdcExtrasUntouched();
        assertMdc(null, null, null);

        try (LoggingContext loggingContext = LoggingContext.forConnector(CONNECTOR_NAME)) {
            assertMdc(CONNECTOR_NAME, null, Scope.WORKER);
            log.info("Starting Connector");
        }

        assertMdcExtrasUntouched();
        assertMdc(null, null, null);
    }

    @Test
    public void shouldCreateTaskLoggingContext() {
        assertMdcExtrasUntouched();
        try (LoggingContext loggingContext = LoggingContext.forTask(TASK_ID1)) {
            assertMdc(TASK_ID1.connector(), TASK_ID1.task(), Scope.TASK);
            log.info("Running task");
        }

        assertMdcExtrasUntouched();
        assertMdc(null, null, null);
    }

    @Test
    public void shouldCreateOffsetsLoggingContext() {
        assertMdcExtrasUntouched();
        try (LoggingContext loggingContext = LoggingContext.forOffsets(TASK_ID1)) {
            assertMdc(TASK_ID1.connector(), TASK_ID1.task(), Scope.OFFSETS);
            log.info("Running task");
        }

        assertMdcExtrasUntouched();
        assertMdc(null, null, null);
    }

    @Test
    public void shouldAllowNestedLoggingContexts() {
        assertMdcExtrasUntouched();
        assertMdc(null, null, null);
        try (LoggingContext loggingContext1 = LoggingContext.forConnector(CONNECTOR_NAME)) {
            assertMdc(CONNECTOR_NAME, null, Scope.WORKER);
            log.info("Starting Connector");
            // Set the extra MDC parameter, as if the connector were
            MDC.put(EXTRA_KEY3, EXTRA_VALUE3);
            assertConnectorMdcSet();

            try (LoggingContext loggingContext2 = LoggingContext.forTask(TASK_ID1)) {
                assertMdc(TASK_ID1.connector(), TASK_ID1.task(), Scope.TASK);
                log.info("Starting task");
                // The extra connector-specific MDC parameter should still be set
                assertConnectorMdcSet();

                try (LoggingContext loggingContext3 = LoggingContext.forOffsets(TASK_ID1)) {
                    assertMdc(TASK_ID1.connector(), TASK_ID1.task(), Scope.OFFSETS);
                    assertConnectorMdcSet();
                    log.info("Offsets for task");
                }

                assertMdc(TASK_ID1.connector(), TASK_ID1.task(), Scope.TASK);
                log.info("Stopping task");
                // The extra connector-specific MDC parameter should still be set
                assertConnectorMdcSet();
            }

            assertMdc(CONNECTOR_NAME, null, Scope.WORKER);
            log.info("Stopping Connector");
            // The extra connector-specific MDC parameter should still be set
            assertConnectorMdcSet();
        }
        assertMdcExtrasUntouched();
        assertMdc(null, null, null);

        // The extra connector-specific MDC parameter should still be set
        assertConnectorMdcSet();

        LoggingContext.clear();
        assertConnectorMdcUnset();
    }

    protected void assertMdc(String connectorName, Integer taskId, Scope scope) {
        String context = MDC.get(LoggingContext.CONNECTOR_CONTEXT);
        if (context != null) {
            assertEquals(
                "Context should begin with connector name when the connector name is non-null",
                connectorName != null,
                context.startsWith("[" + connectorName)
            );
            if (scope != null) {
                assertTrue("Context should contain the scope", context.contains(scope.toString()));
            }
            if (taskId != null) {
                assertTrue("Context should contain the taskId", context.contains(taskId.toString()));
            }
        } else {
            assertNull("No logging context found, expected null connector name", connectorName);
            assertNull("No logging context found, expected null task ID", taskId);
            assertNull("No logging context found, expected null scope", scope);
        }
    }

    protected void assertMdcExtrasUntouched() {
        assertEquals(EXTRA_VALUE1, MDC.get(EXTRA_KEY1));
        assertEquals(EXTRA_VALUE2, MDC.get(EXTRA_KEY2));
    }

    protected void assertConnectorMdcSet() {
        assertEquals(EXTRA_VALUE3, MDC.get(EXTRA_KEY3));
    }

    protected void assertConnectorMdcUnset() {
        assertNull(MDC.get(EXTRA_KEY3));
    }
}