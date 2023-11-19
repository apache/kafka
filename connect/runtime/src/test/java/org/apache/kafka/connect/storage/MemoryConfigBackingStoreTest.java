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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MemoryConfigBackingStoreTest {

    private static final List<String> CONNECTOR_IDS = Arrays.asList("connector1", "connector2");

    // Actual values are irrelevant here and can be used as either connector or task configurations
    private static final List<Map<String, String>> SAMPLE_CONFIGS = Arrays.asList(
        Collections.singletonMap("config-key-one", "config-value-one"),
        Collections.singletonMap("config-key-two", "config-value-two"),
        Collections.singletonMap("config-key-three", "config-value-three")
    );

    @Mock
    private ConfigBackingStore.UpdateListener configUpdateListener;
    private final MemoryConfigBackingStore configStore = new MemoryConfigBackingStore();

    @Before
    public void setUp() {
        configStore.setUpdateListener(configUpdateListener);
    }

    @Test
    public void testPutConnectorConfig() {
        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        ClusterConfigState configState = configStore.snapshot();

        assertTrue(configState.contains(CONNECTOR_IDS.get(0)));
        // Default initial target state of STARTED should be used if no explicit target state is specified
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(1, configState.connectors().size());

        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
    }

    @Test
    public void testPutConnectorConfigWithTargetState() {
        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), TargetState.PAUSED);
        ClusterConfigState configState = configStore.snapshot();

        assertTrue(configState.contains(CONNECTOR_IDS.get(0)));
        assertEquals(TargetState.PAUSED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(1, configState.connectors().size());

        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
        // onConnectorTargetStateChange hook shouldn't be called when a connector is created with a specific initial target state
        verify(configUpdateListener, never()).onConnectorTargetStateChange(eq(CONNECTOR_IDS.get(0)));
    }

    @Test
    public void testPutConnectorConfigUpdateExisting() {
        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        ClusterConfigState configState = configStore.snapshot();

        assertTrue(configState.contains(CONNECTOR_IDS.get(0)));
        // Default initial target state of STARTED should be used if no explicit target state is specified
        assertEquals(TargetState.STARTED, configState.targetState(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(0), configState.connectorConfig(CONNECTOR_IDS.get(0)));
        assertEquals(1, configState.connectors().size());

        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(1), null);
        configState = configStore.snapshot();
        assertEquals(SAMPLE_CONFIGS.get(1), configState.connectorConfig(CONNECTOR_IDS.get(0)));

        verify(configUpdateListener, times(2)).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
    }

    @Test
    public void testRemoveConnectorConfig() {
        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        configStore.putConnectorConfig(CONNECTOR_IDS.get(1), SAMPLE_CONFIGS.get(1), null);
        ClusterConfigState configState = configStore.snapshot();

        Set<String> expectedConnectors = new HashSet<>();
        expectedConnectors.add(CONNECTOR_IDS.get(0));
        expectedConnectors.add(CONNECTOR_IDS.get(1));
        assertEquals(expectedConnectors, configState.connectors());

        configStore.removeConnectorConfig(CONNECTOR_IDS.get(1));
        configState = configStore.snapshot();

        assertFalse(configState.contains(CONNECTOR_IDS.get(1)));
        assertEquals(1, configState.connectors().size());

        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(1)));
        verify(configUpdateListener).onConnectorConfigRemove(eq(CONNECTOR_IDS.get(1)));
    }

    @Test
    public void testPutTaskConfigs() {
        // Can't write task configs for non-existent connector
        assertThrows(IllegalArgumentException.class,
            () -> configStore.putTaskConfigs(CONNECTOR_IDS.get(0), Collections.singletonList(SAMPLE_CONFIGS.get(1))));

        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        configStore.putTaskConfigs(CONNECTOR_IDS.get(0), Collections.singletonList(SAMPLE_CONFIGS.get(1)));
        ClusterConfigState configState = configStore.snapshot();

        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_IDS.get(0), 0);
        assertEquals(1, configState.taskCount(CONNECTOR_IDS.get(0)));
        assertEquals(SAMPLE_CONFIGS.get(1), configState.taskConfig(taskId));

        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
        verify(configUpdateListener).onTaskConfigUpdate(eq(Collections.singleton(taskId)));
    }

    @Test
    public void testRemoveTaskConfigs() {
        // Can't remove task configs for non-existent connector
        assertThrows(IllegalArgumentException.class,
            () -> configStore.removeTaskConfigs(CONNECTOR_IDS.get(0)));

        // This workaround is required to verify the arguments passed to ConfigBackingStore.UpdateListener::onTaskConfigUpdate because Mockito
        // records references to the collection instead of a copy and the argument passed to the method the first time in
        // MemoryConfigBackingStore::putTaskConfigs is cleared in MemoryConfigBackingStore::removeTaskConfigs
        final List<Collection<ConnectorTaskId>> onTaskConfigUpdateCaptures = new ArrayList<>();
        doAnswer(invocation -> {
            onTaskConfigUpdateCaptures.add(new HashSet<>(invocation.getArgument(0)));
            return null;
        }).when(configUpdateListener).onTaskConfigUpdate(anySet());

        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        configStore.putTaskConfigs(CONNECTOR_IDS.get(0), Collections.singletonList(SAMPLE_CONFIGS.get(1)));
        configStore.removeTaskConfigs(CONNECTOR_IDS.get(0));
        ClusterConfigState configState = configStore.snapshot();

        assertEquals(0, configState.taskCount(CONNECTOR_IDS.get(0)));
        assertEquals(Collections.emptyList(), configState.tasks(CONNECTOR_IDS.get(0)));

        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
        verify(configUpdateListener, times(2)).onTaskConfigUpdate(anySet());

        ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_IDS.get(0), 0);
        assertEquals(Arrays.asList(Collections.singleton(taskId), Collections.singleton(taskId)), onTaskConfigUpdateCaptures);
    }

    @Test
    public void testPutTargetState() {
        // Can't write target state for non-existent connector
        assertThrows(IllegalArgumentException.class, () -> configStore.putTargetState(CONNECTOR_IDS.get(0), TargetState.PAUSED));

        configStore.putConnectorConfig(CONNECTOR_IDS.get(0), SAMPLE_CONFIGS.get(0), null);
        configStore.putTargetState(CONNECTOR_IDS.get(0), TargetState.PAUSED);
        // Ensure that ConfigBackingStore.UpdateListener::onConnectorTargetStateChange is called only once if the same state is written twice
        configStore.putTargetState(CONNECTOR_IDS.get(0), TargetState.PAUSED);
        ClusterConfigState configState = configStore.snapshot();

        assertEquals(TargetState.PAUSED, configState.targetState(CONNECTOR_IDS.get(0)));

        verify(configUpdateListener).onConnectorConfigUpdate(eq(CONNECTOR_IDS.get(0)));
        verify(configUpdateListener).onConnectorTargetStateChange(eq(CONNECTOR_IDS.get(0)));
    }
}
