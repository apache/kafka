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
package org.apache.kafka.tools;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class JmxToolTestFixtureTest {
    private final MBeanServer server = MBeanServerFactory.newMBeanServer();
    private final ObjectName metricsName = objectName();
    private final JMXConnectorServer connector = mock(JMXConnectorServer.class);
    private final JmxToolTest.JmxFixture fixture = new JmxToolTest.JmxFixture(server, metricsName);

    private static ObjectName objectName() {
        try {
            return new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec");
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void testCloseRemovesMetricsAndAllowsReregistration() throws Exception {
        fixture.start(() -> connector);
        assertTrue(server.isRegistered(metricsName));
        assertEquals(3.0, server.getAttribute(metricsName, "FiveMinuteRate"));

        fixture.close();

        assertFalse(server.isRegistered(metricsName));
        verify(connector).stop();
        server.registerMBean(new JmxToolTest.Metrics(), metricsName);
        assertTrue(server.isRegistered(metricsName));
    }

    @Test
    void testRegistrationFailurePreservesExistingMetrics() throws Exception {
        JmxToolTest.Metrics existing = new JmxToolTest.Metrics() {
            @Override
            public double getFiveMinuteRate() {
                return 7.0;
            }
        };
        server.registerMBean(existing, metricsName);

        assertThrows(InstanceAlreadyExistsException.class, () -> fixture.start(() -> connector));
        fixture.close();

        assertEquals(7.0, server.getAttribute(metricsName, "FiveMinuteRate"));
        verifyNoInteractions(connector);
    }

    @Test
    void testConnectorCreationFailureRemovesMetrics() {
        IOException failure = new IOException("creation failed");

        assertSame(failure, assertThrows(IOException.class, () -> fixture.start(() -> {
            throw failure;
        })));

        assertFalse(server.isRegistered(metricsName));
    }

    @Test
    void testConnectorStartFailureRemovesMetrics() throws Exception {
        IOException failure = new IOException("start failed");
        doThrow(failure).when(connector).start();

        assertSame(failure, assertThrows(IOException.class, () -> fixture.start(() -> connector)));

        assertFalse(server.isRegistered(metricsName));
        verify(connector).stop();
    }

    @Test
    void testConnectorStopFailureStillRemovesMetrics() throws Exception {
        fixture.start(() -> connector);
        IOException failure = new IOException("stop failed");
        doThrow(failure).when(connector).stop();

        assertSame(failure, assertThrows(IOException.class, fixture::close));

        assertFalse(server.isRegistered(metricsName));
    }

    @Test
    void testStartFailurePreservesCleanupFailure() throws Exception {
        IOException startFailure = new IOException("start failed");
        IOException stopFailure = new IOException("stop failed");
        doThrow(startFailure).when(connector).start();
        doThrow(stopFailure).when(connector).stop();

        assertSame(startFailure, assertThrows(IOException.class, () -> fixture.start(() -> connector)));

        assertFalse(server.isRegistered(metricsName));
        assertEquals(1, startFailure.getSuppressed().length);
        assertSame(stopFailure, startFailure.getSuppressed()[0]);
    }

    @Test
    void testRepeatedClosePreservesReplacementMetrics() throws Exception {
        fixture.start(() -> connector);
        fixture.close();
        server.registerMBean(new JmxToolTest.Metrics(), metricsName);

        fixture.close();

        assertTrue(server.isRegistered(metricsName));
        verify(connector).stop();
    }

    @Test
    void testCloseBeforeStart() throws Exception {
        fixture.close();

        assertFalse(server.isRegistered(metricsName));
        verifyNoInteractions(connector);
    }
}
