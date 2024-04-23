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

package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdminMetadataManagerTest {
    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final long refreshBackoffMs = 100;
    private final long metadataExpireMs = 60000;
    private final AdminMetadataManager mgr = new AdminMetadataManager(
            logContext, refreshBackoffMs, metadataExpireMs, false);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSetUsingBootstrapControllers(boolean usingBootstrapControllers) {
        AdminMetadataManager manager = new AdminMetadataManager(
                logContext, refreshBackoffMs, metadataExpireMs, usingBootstrapControllers);
        assertEquals(usingBootstrapControllers, manager.usingBootstrapControllers());
    }

    @Test
    public void testMetadataReady() {
        // Metadata is not ready on initialization
        assertFalse(mgr.isReady());
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()));

        // Metadata is not ready when bootstrap servers are set
        mgr.update(Cluster.bootstrap(Collections.singletonList(new InetSocketAddress("localhost", 9999))),
                time.milliseconds());
        assertFalse(mgr.isReady());
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()));

        mgr.update(mockCluster(), time.milliseconds());
        assertTrue(mgr.isReady());
        assertEquals(metadataExpireMs, mgr.metadataFetchDelayMs(time.milliseconds()));

        time.sleep(metadataExpireMs);
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()));
    }

    @Test
    public void testMetadataRefreshBackoff() {
        mgr.transitionToUpdatePending(time.milliseconds());
        assertEquals(Long.MAX_VALUE, mgr.metadataFetchDelayMs(time.milliseconds()));

        mgr.updateFailed(new RuntimeException());
        assertEquals(refreshBackoffMs, mgr.metadataFetchDelayMs(time.milliseconds()));

        // Even if we explicitly request an update, the backoff should be respected
        mgr.requestUpdate();
        assertEquals(refreshBackoffMs, mgr.metadataFetchDelayMs(time.milliseconds()));

        time.sleep(refreshBackoffMs);
        assertEquals(0, mgr.metadataFetchDelayMs(time.milliseconds()));
    }

    @Test
    public void testAuthenticationFailure() {
        mgr.transitionToUpdatePending(time.milliseconds());
        mgr.updateFailed(new AuthenticationException("Authentication failed"));
        assertEquals(refreshBackoffMs, mgr.metadataFetchDelayMs(time.milliseconds()));
        assertThrows(AuthenticationException.class, mgr::isReady);
        mgr.update(mockCluster(), time.milliseconds());
        assertTrue(mgr.isReady());
    }

    private static Cluster mockCluster() {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        nodes.put(1, new Node(1, "localhost", 8122));
        nodes.put(2, new Node(2, "localhost", 8123));
        return new Cluster("mockClusterId", nodes.values(),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));
    }

}
