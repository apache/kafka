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
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectStandalone;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class StandaloneWorkerIntegrationTest {

    private EmbeddedConnectStandalone connect;

    @Before
    public void setup() {
        connect = new EmbeddedConnectStandalone.Builder()
                .build();
        connect.start();
    }

    @After
    public void cleanup() {
        connect.stop();
    }

    @Test
    public void testDynamicLogging() {
        Map<String, LoggerLevel> initialLevels = connect.allLogLevels();
        assertFalse("Connect REST API did not list any known loggers", initialLevels.isEmpty());
        Map<String, LoggerLevel> invalidModifiedLoggers = Utils.filterMap(
                initialLevels,
                StandaloneWorkerIntegrationTest::isModified
        );
        assertEquals(
                "No loggers should have a non-null last-modified timestamp",
                Collections.emptyMap(),
                invalidModifiedLoggers
        );

        // Tests with no scope
        // The current level may match the first level we set the namespace to,
        // so we issue a preliminary request with a different level to guarantee that a
        // change takes place and that the last modified timestamp should be non-null
        final String namespace1 = "org.apache.kafka.connect";
        final String level1 = "DEBUG";
        connect.setLogLevel(namespace1, "ERROR", null);
        Map<String, LoggerLevel> currentLevels = testSetLoggingLevel(namespace1, level1, null, initialLevels);

        // Tests with scope=worker
        final String namespace2 = "org.apache.kafka.clients";
        final String level2 = "INFO";
        connect.setLogLevel(namespace2, "WARN", "worker");
        currentLevels = testSetLoggingLevel(namespace2, level2, "worker", currentLevels);

        LoggerLevel priorLoggerLevel = connect.getLogLevel(namespace2);
        connect.setLogLevel(namespace2, level2, "worker");
        LoggerLevel currentLoggerLevel = connect.getLogLevel(namespace2);
        assertEquals(
                "Log level and last-modified timestamp should not be affected by consecutive identical requests",
                priorLoggerLevel,
                currentLoggerLevel
        );

        // Tests with scope=cluster
        final String namespace3 = "org.apache.kafka.streams";
        final String level3 = "TRACE";
        connect.setLogLevel(namespace3, "DEBUG", "cluster");
        testSetLoggingLevel(namespace3, level3, "cluster", currentLevels);
    }

    private Map<String, LoggerLevel> testSetLoggingLevel(
            String namespace,
            String level,
            String scope,
            Map<String, LoggerLevel> initialLevels
    ) {
        long requestTime = System.currentTimeMillis();
        List<String> affectedLoggers = connect.setLogLevel(namespace, level, scope);
        if ("cluster".equals(scope)) {
            assertNull(
                    "Modifying log levels with scope=cluster should result in an empty response",
                    affectedLoggers
            );
        } else {
            assertTrue(affectedLoggers.contains(namespace));
            List<String> invalidAffectedLoggers = affectedLoggers.stream()
                    .filter(l -> !l.startsWith(namespace))
                    .collect(Collectors.toList());
            assertEquals(
                    "No loggers outside the namespace '" + namespace
                            + "' should have been included in the response for a request to modify that namespace",
                    Collections.emptyList(),
                    invalidAffectedLoggers
            );
        }

        // Verify the information for this single logger

        LoggerLevel loggerLevel = connect.getLogLevel(namespace);
        assertNotNull(loggerLevel);
        assertEquals(level, loggerLevel.level());
        assertNotNull(loggerLevel.lastModified());
        assertTrue(
                "Last-modified timestamp for logger level is " + loggerLevel.lastModified()
                        + ", which is before " + requestTime + ", the most-recent time the level was adjusted",
                loggerLevel.lastModified() >= requestTime
        );

        // Verify information for all listed loggers

        Map<String, LoggerLevel> newLevels = connect.allLogLevels();

        Map<String, LoggerLevel> invalidAffectedLoggerLevels = Utils.filterMap(
                newLevels,
                e -> hasNamespace(e, namespace)
                        && (!level(e).equals(level)
                            || !isModified(e)
                            || lastModified(e) < requestTime
                        )
        );
        assertEquals(
                "At least one logger in the affected namespace '" + namespace
                        + "' does not have the expected level of '" + level
                        + "', has a null last-modified timestamp, or has a last-modified timestamp "
                        + "that is less recent than " + requestTime
                        + ", which is when the namespace was last adjusted",
                Collections.emptyMap(),
                invalidAffectedLoggerLevels
        );

        Set<String> droppedLoggers = Utils.diff(HashSet::new, initialLevels.keySet(), newLevels.keySet());
        assertEquals(
                "At least one logger was present in the listing of all loggers "
                        + "before the logging level for namespace '" + namespace
                        + "' was set to '" + level
                        + "' that is no longer present",
                Collections.emptySet(),
                droppedLoggers
        );

        Map<String, LoggerLevel> invalidUnaffectedLoggerLevels = Utils.filterMap(
                newLevels,
                e -> !hasNamespace(e, namespace) && !e.getValue().equals(initialLevels.get(e.getKey()))
        );
        assertEquals(
                "At least one logger outside of the affected namespace '" + namespace
                        + "' has a different logging level or last-modified timestamp than it did "
                        + "before the namespace was set to level '" + level
                        + "'; none of these loggers should have been affected",
                Collections.emptyMap(),
                invalidUnaffectedLoggerLevels
        );

        return newLevels;
    }

    private static boolean hasNamespace(Map.Entry<String, ?> entry, String namespace) {
        return entry.getKey().startsWith(namespace);
    }

    private static boolean isModified(Map.Entry<?, LoggerLevel> entry) {
        return lastModified(entry) != null;
    }

    private static Long lastModified(Map.Entry<?, LoggerLevel> entry) {
        return entry.getValue().lastModified();
    }

    private static String level(Map.Entry<?, LoggerLevel> entry) {
        return entry.getValue().level();
    }

}
