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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.VersionedPluginLoadingException;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachedConnectorTest {

    private final Plugins plugin = mock(Plugins.class);

    public void testConnectorCaching(VersionRange range) throws Exception {
        CachedConnectors cachedConnectors = new CachedConnectors(plugin);
        for (Connector connector : Arrays.asList(new SampleSinkConnector(), new SampleSourceConnector())) {

            when(plugin.newConnector(connector.getClass().getName(), range)).thenReturn(connector);
            Connector cachedConnector = cachedConnectors.getConnector(connector.getClass().getName(), range);
            Assertions.assertEquals(connector, cachedConnector);

            cachedConnector = cachedConnectors.getConnector(connector.getClass().getName(), range);
            Assertions.assertEquals(connector, cachedConnector);

            verify(plugin, times(1)).newConnector(connector.getClass().getName(), range);
            reset(plugin);
        }
    }

    @SuppressWarnings("unchecked, rawtypes")
    public void testInvalidConnectorCaching(Class exceptionClass, VersionRange range) {
        CachedConnectors cachedConnectors = new CachedConnectors(plugin);
        when(plugin.newConnector("invalid", range)).thenThrow(exceptionClass);
        Assertions.assertThrows(exceptionClass, () -> cachedConnectors.getConnector("invalid", range));

        Assertions.assertThrows(exceptionClass, () -> cachedConnectors.getConnector("invalid", range));
        verify(plugin, times(1)).newConnector("invalid", range);
    }

    @Test
    public void TestConnectorCachingNoVersion() throws Exception {
        testConnectorCaching(null);
    }

    @Test
    public void TestConnectorCachingWithVersion() throws Exception {
        testConnectorCaching(VersionRange.createFromVersionSpec("1.0"));
    }

    @Test
    public void TestInvalidConnectorCaching() throws InvalidVersionSpecificationException {
        testInvalidConnectorCaching(ConnectException.class, null);
        testInvalidConnectorCaching(ConnectException.class, VersionRange.createFromVersionSpec("1.0"));
    }

    @Test
    public void TestInvalidVersionedConnectorCaching() throws InvalidVersionSpecificationException {
        testInvalidConnectorCaching(VersionedPluginLoadingException.class, VersionRange.createFromVersionSpec("1.0"));
    }

    @Test
    public void TestMultipleVersionedConnectorCaching() throws Exception {
        CachedConnectors cachedConnectors = new CachedConnectors(plugin);
        VersionRange v1 = VersionRange.createFromVersionSpec("1.0");
        VersionRange v2 = VersionRange.createFromVersionSpec("2.0");
        VersionRange v3 = VersionRange.createFromVersionSpec("3.0");
        when(plugin.newConnector("test.connector", v1)).thenReturn(new SampleSinkConnector());
        when(plugin.newConnector("test.connector", v2)).thenReturn(new SampleSinkConnector());
        when(plugin.newConnector("test.connector", v3)).thenThrow(VersionedPluginLoadingException.class);

        for (int i = 0; i < 2; i++) {
            cachedConnectors.getConnector("test.connector", v1);
            cachedConnectors.getConnector("test.connector", v2);
            Assertions.assertThrows(VersionedPluginLoadingException.class, () -> cachedConnectors.getConnector("test.connector", v3));
        }
        verify(plugin, times(1)).newConnector("test.connector", v1);
        verify(plugin, times(1)).newConnector("test.connector", v2);
        verify(plugin, times(1)).newConnector("test.connector", v3);
    }
}
