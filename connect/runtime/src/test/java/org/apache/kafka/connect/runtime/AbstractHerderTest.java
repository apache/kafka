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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractHerderTest extends EasyMockSupport {
    private final Worker worker = strictMock(Worker.class);
    private final String workerId = "workerId";
    private final int generation = 5;
    private final String connector = "connector";
    private final Plugins plugins = strictMock(Plugins.class);
    private final ClassLoader classLoader = strictMock(ClassLoader.class);

    @Test
    public void connectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);

        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, StatusBackingStore.class, ConfigBackingStore.class)
                .withArgs(worker, workerId, statusStore, configStore)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.config(connector)).andReturn(null);

        EasyMock.expect(statusStore.get(connector))
                .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        EasyMock.expect(statusStore.getAll(connector))
                .andReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);

        replayAll();


        ConnectorStateInfo state = herder.connectorStatus(connector);

        assertEquals(connector, state.name());
        assertEquals("RUNNING", state.connector().state());
        assertEquals(1, state.tasks().size());
        assertEquals(workerId, state.connector().workerId());

        ConnectorStateInfo.TaskState taskState = state.tasks().get(0);
        assertEquals(0, taskState.id());
        assertEquals("UNASSIGNED", taskState.state());
        assertEquals(workerId, taskState.workerId());

        verifyAll();
    }

    @Test
    public void taskStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        String workerId = "workerId";

        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, StatusBackingStore.class, ConfigBackingStore.class)
                .withArgs(worker, workerId, statusStore, configStore)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(5);

        final Capture<TaskStatus> statusCapture = EasyMock.newCapture();
        statusStore.putSafe(EasyMock.capture(statusCapture));
        EasyMock.expectLastCall();

        EasyMock.expect(statusStore.get(taskId)).andAnswer(new IAnswer<TaskStatus>() {
            @Override
            public TaskStatus answer() throws Throwable {
                return statusCapture.getValue();
            }
        });

        replayAll();

        herder.onFailure(taskId, new RuntimeException());

        ConnectorStateInfo.TaskState taskState = herder.taskStatus(taskId);
        assertEquals(workerId, taskState.workerId());
        assertEquals("FAILED", taskState.state());
        assertEquals(0, taskState.id());
        assertNotNull(taskState.trace());

        verifyAll();
    }


    @Test(expected = BadRequestException.class)
    public void testConfigValidationEmptyConfig() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class);
        replayAll();

        herder.validateConnectorConfig(new HashMap<String, String>());

        verifyAll();
    }

    @Test()
    public void testConfigValidationMissingName() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class);
        replayAll();

        Map<String, String> config = Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSourceConnector.class.getName());
        ConfigInfos result = herder.validateConnectorConfig(config);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(TestSourceConnector.class.getName(), result.name());
        assertEquals(Arrays.asList(ConnectorConfig.COMMON_GROUP, ConnectorConfig.TRANSFORMS_GROUP), result.groups());
        assertEquals(2, result.errorCount());
        // Base connector config has 6 fields, connector's configs add 2
        assertEquals(8, result.values().size());
        // Missing name should generate an error
        assertEquals(ConnectorConfig.NAME_CONFIG, result.values().get(0).configValue().name());
        assertEquals(1, result.values().get(0).configValue().errors().size());
        // "required" config from connector should generate an error
        assertEquals("required", result.values().get(6).configValue().name());
        assertEquals(1, result.values().get(6).configValue().errors().size());

        verifyAll();
    }

    @Test()
    public void testConfigValidationTransformsExtendResults() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class);

        // 2 transform aliases defined -> 2 plugin lookups
        Set<PluginDesc<Transformation>> transformations = new HashSet<>();
        transformations.add(new PluginDesc<Transformation>(SampleTransformation.class, "1.0", classLoader));
        EasyMock.expect(plugins.transformations()).andReturn(transformations).times(2);

        replayAll();

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA,xformB");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(TestSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                "Transforms: xformA",
                "Transforms: xformB"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(2, result.errorCount());
        // Base connector config has 6 fields, connector's configs add 2, 2 type fields from the transforms, and
        // 1 from the valid transformation's config
        assertEquals(11, result.values().size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type", result.values().get(6).configValue().name());
        assertTrue(result.values().get(6).configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig", result.values().get(7).configValue().name());
        assertEquals("transforms.xformB.type", result.values().get(8).configValue().name());
        assertFalse(result.values().get(8).configValue().errors().isEmpty());

        verifyAll();
    }

    private AbstractHerder createConfigValidationHerder(Class<? extends Connector> connectorClass) {


        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, StatusBackingStore.class, ConfigBackingStore.class)
                .withArgs(worker, workerId, statusStore, configStore)
                .addMockedMethod("generation")
                .createMock();
        EasyMock.expect(herder.generation()).andStubReturn(generation);

        // Call to validateConnectorConfig
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);
        final Connector connector;
        try {
            connector = connectorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Couldn't create connector", e);
        }
        EasyMock.expect(plugins.newConnector(connectorClass.getName())).andReturn(connector);
        EasyMock.expect(plugins.compareAndSwapLoaders(connector)).andReturn(classLoader);
        return herder;
    }

    public static class SampleTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
        @Override
        public void configure(Map<String, ?> configs) {

        }

        @Override
        public R apply(R record) {
            return record;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("subconfig", ConfigDef.Type.STRING, "default", ConfigDef.Importance.LOW, "docs");
        }

        @Override
        public void close() {

        }
    }
}
