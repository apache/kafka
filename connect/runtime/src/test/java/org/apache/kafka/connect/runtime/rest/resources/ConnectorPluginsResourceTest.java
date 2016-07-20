/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.tools.MockConnector;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.tools.SchemaSourceConnector;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestServer.class)
@PowerMockIgnore("javax.management.*")
public class ConnectorPluginsResourceTest {

    private static Map<String, String> props = new HashMap<>();
    static {
        props.put("name", "test");
        props.put("test.string.config", "testString");
        props.put("test.int.config", "1");
        props.put("test.list.config", "a,b");
    }

    private static final ConfigInfos CONFIG_INFOS;
    private static final int ERROR_COUNT = 1;

    static {
        List<ConfigInfo> configs = new LinkedList<>();

        ConfigDef connectorConfigDef = ConnectorConfig.configDef();
        List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(props);
        ConfigInfos result = AbstractHerder.generateResult(ConnectorPluginsResourceTestConnector.class.getName(), connectorConfigDef.configKeys(), connectorConfigValues, Collections.<String>emptyList());
        configs.addAll(result.values());

        ConfigKeyInfo configKeyInfo = new ConfigKeyInfo("test.string.config", "STRING", true, "", "HIGH", "Test configuration for string type.", null, -1, "NONE", "test.string.config", Collections.<String>emptyList());
        ConfigValueInfo configValueInfo = new ConfigValueInfo("test.string.config", "testString", Collections.<String>emptyList(), Collections.<String>emptyList(), true);
        ConfigInfo configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.int.config", "INT", true, "", "MEDIUM", "Test configuration for integer type.", "Test", 1, "MEDIUM", "test.int.config", Collections.<String>emptyList());
        configValueInfo = new ConfigValueInfo("test.int.config", "1", Arrays.asList("1", "2", "3"), Collections.<String>emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.string.config.default", "STRING", false, "", "LOW", "Test configuration with default value.", null, -1, "NONE", "test.string.config.default", Collections.<String>emptyList());
        configValueInfo = new ConfigValueInfo("test.string.config.default", "", Collections.<String>emptyList(), Collections.<String>emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.list.config", "LIST", true, "", "HIGH", "Test configuration for list type.", "Test", 2, "LONG", "test.list.config", Collections.<String>emptyList());
        configValueInfo = new ConfigValueInfo("test.list.config", "a,b", Arrays.asList("a", "b", "c"), Collections.<String>emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);

        CONFIG_INFOS = new ConfigInfos(ConnectorPluginsResourceTestConnector.class.getName(), ERROR_COUNT, Collections.singletonList("Test"), configs);
    }

    @Mock
    private Herder herder;
    private ConnectorPluginsResource connectorPluginsResource;

    @Before
    public void setUp() throws NoSuchMethodException {
        PowerMock.mockStatic(RestServer.class,
                             RestServer.class.getMethod("httpRequest", String.class, String.class, Object.class, TypeReference.class));
        connectorPluginsResource = new ConnectorPluginsResource(herder);
    }

    @Test
    public void testValidateConfig() throws Throwable {
        herder.validateConfigs(EasyMock.eq(ConnectorPluginsResourceTestConnector.class.getName()), EasyMock.eq(props));

        PowerMock.expectLastCall().andAnswer(new IAnswer<ConfigInfos>() {
            @Override
            public ConfigInfos answer() {
                ConfigDef connectorConfigDef = ConnectorConfig.configDef();
                List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(props);

                Connector connector = new ConnectorPluginsResourceTestConnector();
                Config config = connector.validate(props);
                ConfigDef configDef = connector.config();
                Map<String, ConfigDef.ConfigKey> configKeys = configDef.configKeys();
                List<ConfigValue> configValues = config.configValues();

                Map<String, ConfigDef.ConfigKey> resultConfigKeys = new HashMap<>(configKeys);
                resultConfigKeys.putAll(connectorConfigDef.configKeys());
                configValues.addAll(connectorConfigValues);

                return AbstractHerder.generateResult(ConnectorPluginsResourceTestConnector.class.getName(), resultConfigKeys, configValues, Collections.singletonList("Test"));
            }
        });
        PowerMock.replayAll();

        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(ConnectorPluginsResourceTestConnector.class.getName(), props);
        assertEquals(CONFIG_INFOS.name(), configInfos.name());
        assertEquals(CONFIG_INFOS.errorCount(), configInfos.errorCount());
        assertEquals(CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(new HashSet<>(CONFIG_INFOS.values()), new HashSet<>(configInfos.values()));

        PowerMock.verifyAll();
    }

    @Test
    public void testListConnectorPlugins() {
        Set<ConnectorPluginInfo> connectorPlugins = new HashSet<>(connectorPluginsResource.listConnectorPlugins());
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(Connector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(SourceConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(SinkConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(VerifiableSourceConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(VerifiableSinkConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(MockSourceConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(MockSinkConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(MockConnector.class.getCanonicalName())));
        assertFalse(connectorPlugins.contains(new ConnectorPluginInfo(SchemaSourceConnector.class.getCanonicalName())));
        assertTrue(connectorPlugins.contains(new ConnectorPluginInfo(ConnectorPluginsResourceTestConnector.class.getCanonicalName())));
    }


    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class ConnectorPluginsResourceTestConnector extends Connector {

        private static final String TEST_STRING_CONFIG = "test.string.config";
        private static final String TEST_INT_CONFIG = "test.int.config";
        private static final String TEST_STRING_CONFIG_DEFAULT = "test.string.config.default";
        private static final String TEST_LIST_CONFIG = "test.list.config";
        private static final String GROUP = "Test";

        private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TEST_STRING_CONFIG, Type.STRING, Importance.HIGH, "Test configuration for string type.")
            .define(TEST_INT_CONFIG, Type.INT, Importance.MEDIUM, "Test configuration for integer type.", GROUP, 1, Width.MEDIUM, TEST_INT_CONFIG, new IntegerRecommender())
            .define(TEST_STRING_CONFIG_DEFAULT, Type.STRING, "", Importance.LOW, "Test configuration with default value.")
            .define(TEST_LIST_CONFIG, Type.LIST, Importance.HIGH, "Test configuration for list type.", GROUP, 2, Width.LONG, TEST_LIST_CONFIG, new ListRecommender());

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }
    }

    private static class IntegerRecommender implements Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return Arrays.<Object>asList(1, 2, 3);
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private static class ListRecommender implements Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return Arrays.<Object>asList("a", "b", "c");
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }
}
