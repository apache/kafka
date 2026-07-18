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
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.VersionedPluginLoadingException;

import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CachedConnectorsTest {

    @Mock
    private Plugins plugins;

    @Test
    public void testCachesConnectorsByClassAndVersion() throws InvalidVersionSpecificationException {
        String sinkConnectorClass = SampleSinkConnector.class.getName();
        String sourceConnectorClass = SampleSourceConnector.class.getName();
        VersionRange firstVersion = VersionRange.createFromVersionSpec("1.0.0");
        VersionRange secondVersion = VersionRange.createFromVersionSpec("2.0.0");
        // Distinct instances verify that the cache separates entries by connector class and version.
        Connector latestSinkConnector = new SampleSinkConnector();
        Connector firstSinkConnector = new SampleSinkConnector();
        Connector secondSinkConnector = new SampleSinkConnector();
        Connector firstSourceConnector = new SampleSourceConnector();
        when(plugins.newConnector(sinkConnectorClass, null)).thenReturn(latestSinkConnector);
        when(plugins.newConnector(sinkConnectorClass, firstVersion)).thenReturn(firstSinkConnector);
        when(plugins.newConnector(sinkConnectorClass, secondVersion)).thenReturn(secondSinkConnector);
        when(plugins.newConnector(sourceConnectorClass, firstVersion)).thenReturn(firstSourceConnector);

        CachedConnectors cachedConnectors = new CachedConnectors(plugins);

        assertSame(latestSinkConnector, cachedConnectors.getConnector(sinkConnectorClass, null));
        assertSame(firstSinkConnector, cachedConnectors.getConnector(sinkConnectorClass, firstVersion));
        assertSame(secondSinkConnector, cachedConnectors.getConnector(sinkConnectorClass, secondVersion));
        assertSame(firstSourceConnector, cachedConnectors.getConnector(sourceConnectorClass, firstVersion));

        assertSame(latestSinkConnector, cachedConnectors.getConnector(sinkConnectorClass, null));
        assertSame(firstSinkConnector, cachedConnectors.getConnector(sinkConnectorClass, firstVersion));
        assertSame(secondSinkConnector, cachedConnectors.getConnector(sinkConnectorClass, secondVersion));
        assertSame(firstSourceConnector, cachedConnectors.getConnector(sourceConnectorClass, firstVersion));

        verify(plugins, times(1)).newConnector(sinkConnectorClass, null);
        verify(plugins, times(1)).newConnector(sinkConnectorClass, firstVersion);
        verify(plugins, times(1)).newConnector(sinkConnectorClass, secondVersion);
        verify(plugins, times(1)).newConnector(sourceConnectorClass, firstVersion);
    }

    @Test
    public void testCachesInvalidConnectorAcrossVersions() throws InvalidVersionSpecificationException {
        String connectorClass = "org.apache.kafka.connect.runtime.InvalidConnector";
        VersionRange requestedVersion = VersionRange.createFromVersionSpec("1.0.0");
        when(plugins.newConnector(connectorClass, null)).thenThrow(new ConnectException("unable to load connector"));

        CachedConnectors cachedConnectors = new CachedConnectors(plugins);

        // A loading failure that is not specific to a version is cached for the connector class and applies to all versions.
        assertThrows(ConnectException.class, () -> cachedConnectors.getConnector(connectorClass, null));
        assertThrows(ConnectException.class, () -> cachedConnectors.getConnector(connectorClass, requestedVersion));

        verify(plugins, times(1)).newConnector(connectorClass, null);
        verify(plugins, never()).newConnector(connectorClass, requestedVersion);
    }

    @Test
    public void testVersionLoadingFailureDoesNotAffectOtherCachedVersions() throws InvalidVersionSpecificationException {
        String connectorClass = SampleSinkConnector.class.getName();
        VersionRange availableVersion = VersionRange.createFromVersionSpec("1.0.0");
        VersionRange unavailableVersion = VersionRange.createFromVersionSpec("2.0.0");
        Connector availableConnector = new SampleSinkConnector();
        when(plugins.newConnector(connectorClass, availableVersion)).thenReturn(availableConnector);
        when(plugins.newConnector(connectorClass, unavailableVersion)).thenThrow(new VersionedPluginLoadingException("unable to load connector version"));

        CachedConnectors cachedConnectors = new CachedConnectors(plugins);

        // Successful and failed version lookups are cached independently.
        assertSame(availableConnector, cachedConnectors.getConnector(connectorClass, availableVersion));
        assertThrows(VersionedPluginLoadingException.class, () -> cachedConnectors.getConnector(connectorClass, unavailableVersion));

        assertSame(availableConnector, cachedConnectors.getConnector(connectorClass, availableVersion));
        assertThrows(VersionedPluginLoadingException.class, () -> cachedConnectors.getConnector(connectorClass, unavailableVersion));

        verify(plugins, times(1)).newConnector(connectorClass, availableVersion);
        verify(plugins, times(1)).newConnector(connectorClass, unavailableVersion);
    }

    @Test
    public void testCachedInvalidVersionFailurePreservesAvailableVersions() throws Exception {
        String requestedVersion = "2.0.0";
        String connectorClass = "org.apache.kafka.connect.runtime.SomeConnector";
        List<String> availableVersions = List.of("1.0.0");
        VersionedPluginLoadingException loadingException =
            new VersionedPluginLoadingException("no matching version", availableVersions);
        when(plugins.newConnector(anyString(), any())).thenThrow(loadingException);

        CachedConnectors cachedConnectors = new CachedConnectors(plugins);
        VersionRange versionRange = PluginUtils.connectorVersionRequirement(requestedVersion);

        VersionedPluginLoadingException firstException = assertThrows(
            VersionedPluginLoadingException.class,
            () -> cachedConnectors.getConnector(connectorClass, versionRange)
        );
        VersionedPluginLoadingException cachedException = assertThrows(
            VersionedPluginLoadingException.class,
            () -> cachedConnectors.getConnector(connectorClass, versionRange)
        );

        // A cached version loading failure should preserve its exception details.
        assertEquals(firstException.getMessage(), cachedException.getMessage());
        assertEquals(availableVersions, cachedException.availableVersions());

        // The second lookup comes from the cache, so the loader is only invoked once.
        verify(plugins, times(1)).newConnector(connectorClass, versionRange);
    }
}
