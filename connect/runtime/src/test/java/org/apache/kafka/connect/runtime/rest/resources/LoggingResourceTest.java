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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.event.Level;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class LoggingResourceTest {

    private LoggingResource loggingResource;

    @Mock
    private Herder herder;

    @Before
    public void setup() {
        loggingResource = new LoggingResource(herder);
    }

    @Test
    public void testGetLevelNotFound() {
        final String logger = "org.apache.rostropovich";
        when(herder.loggerLevel(logger)).thenReturn(null);
        assertThrows(
                NotFoundException.class,
                () -> loggingResource.getLogger(logger)
        );
    }

    @Test
    public void testGetLevel() {
        final String logger = "org.apache.kafka.producer";
        final LoggerLevel expectedLevel = new LoggerLevel(Level.WARN.toString(), 976L);
        when(herder.loggerLevel(logger)).thenReturn(expectedLevel);

        Response response = loggingResource.getLogger(logger);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        LoggerLevel actualLevel = (LoggerLevel) response.getEntity();

        assertEquals(
                expectedLevel,
                actualLevel
        );
    }

    @Test
    public void setLevelWithEmptyArgTest() {
        for (String scope : Arrays.asList("worker", "cluster", "N/A", null)) {
            assertThrows(
                    BadRequestException.class,
                    () -> loggingResource.setLevel(
                            "@root",
                            Collections.emptyMap(),
                            scope
                    )
            );
        }
    }

    @Test
    public void setLevelWithInvalidArgTest() {
        for (String scope : Arrays.asList("worker", "cluster", "N/A", null)) {
            assertThrows(
                    NotFoundException.class,
                    () -> loggingResource.setLevel(
                            "@root",
                            Collections.singletonMap("level", "HIGH"),
                            scope
                    )
            );
        }
    }

    @Test
    public void testSetLevelDefaultScope() {
        testSetLevelWorkerScope(null, true);
    }

    @Test
    public void testSetLevelInvalidScope() {
        testSetLevelWorkerScope("kip-976", true);
    }

    @Test
    public void testSetLevelWorkerScope() {
        testSetLevelWorkerScope("worker", false);
    }

    @SuppressWarnings("unchecked")
    private void testSetLevelWorkerScope(String scope, boolean expectWarning) {
        final String logger = "org.apache.kafka.connect";
        final String level = "TRACE";
        final List<String> expectedLoggers = Arrays.asList(
                "org.apache.kafka.connect",
                "org.apache.kafka.connect.runtime.distributed.DistributedHerder"
        );
        when(herder.setWorkerLoggerLevel(logger, level)).thenReturn(expectedLoggers);

        List<String> actualLoggers;
        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(LoggingResource.class)) {
            Response response = loggingResource.setLevel(logger, Collections.singletonMap("level", level), scope);
            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
            actualLoggers = (List<String>) response.getEntity();
            long warningMessages = logCaptureAppender.getEvents().stream()
                    .filter(e -> "WARN".equals(e.getLevel()))
                    .count();
            if (expectWarning) {
                assertEquals(1, warningMessages);
            } else {
                assertEquals(0, warningMessages);
            }
        }

        assertEquals(expectedLoggers, actualLoggers);
    }

    @Test
    public void testSetLevelClusterScope() {
        final String logger = "org.apache.kafka.connect";
        final String level = "TRACE";

        Response response = loggingResource.setLevel(logger, Collections.singletonMap("level", level), "cluster");

        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
        assertNull(response.getEntity());

        verify(herder).setClusterLoggerLevel(logger, level);
    }

}