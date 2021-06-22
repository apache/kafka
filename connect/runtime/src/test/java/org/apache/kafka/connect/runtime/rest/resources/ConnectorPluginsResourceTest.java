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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.core.HttpHeaders;
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
import org.apache.kafka.connect.runtime.TestSinkConnector;
import org.apache.kafka.connect.runtime.TestSourceConnector;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.tools.MockConnector;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.tools.SchemaSourceConnector;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.apache.kafka.connect.util.Callback;
import org.easymock.Capture;
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

import javax.ws.rs.BadRequestException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestClient.class)
@PowerMockIgnore("javax.management.*")
public class ConnectorPluginsResourceTest {

    private static Map<String, String> props;
    private static Map<String, String> partialProps = new HashMap<>();
    static {
        partialProps.put("name", "test");
        partialProps.put("test.string.config", "testString");
        partialProps.put("test.int.config", "1");
        partialProps.put("test.list.config", "a,b");

        props = new HashMap<>(partialProps);
        props.put("connector.class", ConnectorPluginsResourceTestConnector.class.getSimpleName());
        props.put("plugin.path", null);
    }

    private static final ConfigInfos CONFIG_INFOS;
    private static final ConfigInfos PARTIAL_CONFIG_INFOS;
    private static final int ERROR_COUNT = 0;
    private static final int PARTIAL_CONFIG_ERROR_COUNT = 1;
    private static final Set<PluginDesc<Connector>> CONNECTOR_PLUGINS = new TreeSet<>();

    static {
        List<ConfigInfo> configs = new LinkedList<>();
        List<ConfigInfo> partialConfigs = new LinkedList<>();

        ConfigDef connectorConfigDef = ConnectorConfig.configDef();
        List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(props);
        List<ConfigValue> partialConnectorConfigValues = connectorConfigDef.validate(partialProps);
        ConfigInfos result = AbstractHerder.generateResult(ConnectorPluginsResourceTestConnector.class.getName(), connectorConfigDef.configKeys(), connectorConfigValues, Collections.emptyList());
        ConfigInfos partialResult = AbstractHerder.generateResult(ConnectorPluginsResourceTestConnector.class.getName(), connectorConfigDef.configKeys(), partialConnectorConfigValues, Collections.emptyList());
        configs.addAll(result.values());
        partialConfigs.addAll(partialResult.values());

        ConfigKeyInfo configKeyInfo = new ConfigKeyInfo("test.string.config", "STRING", true, null, "HIGH", "Test configuration for string type.", null, -1, "NONE", "test.string.config", Collections.emptyList());
        ConfigValueInfo configValueInfo = new ConfigValueInfo("test.string.config", "testString", Collections.emptyList(), Collections.emptyList(), true);
        ConfigInfo configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.int.config", "INT", true, null, "MEDIUM", "Test configuration for integer type.", "Test", 1, "MEDIUM", "test.int.config", Collections.emptyList());
        configValueInfo = new ConfigValueInfo("test.int.config", "1", Arrays.asList("1", "2", "3"), Collections.emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.string.config.default", "STRING", false, "", "LOW", "Test configuration with default value.", null, -1, "NONE", "test.string.config.default", Collections.emptyList());
        configValueInfo = new ConfigValueInfo("test.string.config.default", "", Collections.emptyList(), Collections.emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.list.config", "LIST", true, null, "HIGH", "Test configuration for list type.", "Test", 2, "LONG", "test.list.config", Collections.emptyList());
        configValueInfo = new ConfigValueInfo("test.list.config", "a,b", Arrays.asList("a", "b", "c"), Collections.emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        CONFIG_INFOS = new ConfigInfos(ConnectorPluginsResourceTestConnector.class.getName(), ERROR_COUNT, Collections.singletonList("Test"), configs);
        PARTIAL_CONFIG_INFOS = new ConfigInfos(ConnectorPluginsResourceTestConnector.class.getName(), PARTIAL_CONFIG_ERROR_COUNT, Collections.singletonList("Test"), partialConfigs);

        Class<?>[] abstractConnectorClasses = {
            Connector.class,
            SourceConnector.class,
            SinkConnector.class
        };

        Class<?>[] connectorClasses = {
            VerifiableSourceConnector.class,
            VerifiableSinkConnector.class,
            MockSourceConnector.class,
            MockSinkConnector.class,
            MockConnector.class,
            SchemaSourceConnector.class,
            ConnectorPluginsResourceTestConnector.class
        };

        try {
            for (Class<?> klass : abstractConnectorClasses) {
                @SuppressWarnings("unchecked")
                MockConnectorPluginDesc pluginDesc = new MockConnectorPluginDesc((Class<? extends Connector>) klass, "0.0.0");
                CONNECTOR_PLUGINS.add(pluginDesc);
            }
            for (Class<?> klass : connectorClasses) {
                @SuppressWarnings("unchecked")
                MockConnectorPluginDesc pluginDesc = new MockConnectorPluginDesc((Class<? extends Connector>) klass);
                CONNECTOR_PLUGINS.add(pluginDesc);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Mock
    private Herder herder;
    @Mock
    private Plugins plugins;
    private ConnectorPluginsResource connectorPluginsResource;

    @Before
    public void setUp() throws Exception {
        PowerMock.mockStatic(RestClient.class,
                RestClient.class.getMethod("httpRequest", String.class, String.class, HttpHeaders.class, Object.class, TypeReference.class, WorkerConfig.class));

        plugins = PowerMock.createMock(Plugins.class);
        herder = PowerMock.createMock(AbstractHerder.class);
        connectorPluginsResource = new ConnectorPluginsResource(herder);
    }

    private void expectPlugins() {
        EasyMock.expect(herder.plugins()).andReturn(plugins);
        EasyMock.expect(plugins.connectors()).andReturn(CONNECTOR_PLUGINS);
        PowerMock.replayAll();
    }

    @Test
    public void testValidateConfigWithSingleErrorDueToMissingConnectorClassname() throws Throwable {
        Capture<Callback<ConfigInfos>> configInfosCallback = EasyMock.newCapture();
        herder.validateConnectorConfig(EasyMock.eq(partialProps), EasyMock.capture(configInfosCallback), EasyMock.anyBoolean());

        PowerMock.expectLastCall().andAnswer((IAnswer<Void>) () -> {
            ConfigDef connectorConfigDef = ConnectorConfig.configDef();
            List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(partialProps);

            Connector connector = new ConnectorPluginsResourceTestConnector();
            Config config = connector.validate(partialProps);
            ConfigDef configDef = connector.config();
            Map<String, ConfigDef.ConfigKey> configKeys = configDef.configKeys();
            List<ConfigValue> configValues = config.configValues();

            Map<String, ConfigDef.ConfigKey> resultConfigKeys = new HashMap<>(configKeys);
            resultConfigKeys.putAll(connectorConfigDef.configKeys());
            configValues.addAll(connectorConfigValues);

            ConfigInfos configInfos = AbstractHerder.generateResult(
                ConnectorPluginsResourceTestConnector.class.getName(),
                resultConfigKeys,
                configValues,
                Collections.singletonList("Test")
            );
            configInfosCallback.getValue().onCompletion(null, configInfos);
            return null;
        });

        PowerMock.replayAll();

        // This call to validateConfigs does not throw a BadRequestException because we've mocked
        // validateConnectorConfig.
        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(
            ConnectorPluginsResourceTestConnector.class.getSimpleName(),
            partialProps
        );
        assertEquals(PARTIAL_CONFIG_INFOS.name(), configInfos.name());
        assertEquals(PARTIAL_CONFIG_INFOS.errorCount(), configInfos.errorCount());
        assertEquals(PARTIAL_CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(
            new HashSet<>(PARTIAL_CONFIG_INFOS.values()),
            new HashSet<>(configInfos.values())
        );

        PowerMock.verifyAll();
    }

    @Test
    public void testValidateConfigWithSimpleName() throws Throwable {
        Capture<Callback<ConfigInfos>> configInfosCallback = EasyMock.newCapture();
        herder.validateConnectorConfig(EasyMock.eq(props), EasyMock.capture(configInfosCallback), EasyMock.anyBoolean());

        PowerMock.expectLastCall().andAnswer((IAnswer<ConfigInfos>) () -> {
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

            ConfigInfos configInfos = AbstractHerder.generateResult(
                    ConnectorPluginsResourceTestConnector.class.getName(),
                    resultConfigKeys,
                    configValues,
                    Collections.singletonList("Test")
            );
            configInfosCallback.getValue().onCompletion(null, configInfos);
            return null;
        });

        PowerMock.replayAll();

        // make a request to connector-plugins resource using just the simple class name.
        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(
            ConnectorPluginsResourceTestConnector.class.getSimpleName(),
            props
        );
        assertEquals(CONFIG_INFOS.name(), configInfos.name());
        assertEquals(0, configInfos.errorCount());
        assertEquals(CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(new HashSet<>(CONFIG_INFOS.values()), new HashSet<>(configInfos.values()));

        PowerMock.verifyAll();
    }

    @Test
    public void testValidateConfigWithAlias() throws Throwable {
        Capture<Callback<ConfigInfos>> configInfosCallback = EasyMock.newCapture();
        herder.validateConnectorConfig(EasyMock.eq(props), EasyMock.capture(configInfosCallback), EasyMock.anyBoolean());

        PowerMock.expectLastCall().andAnswer((IAnswer<ConfigInfos>) () -> {
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

            ConfigInfos configInfos = AbstractHerder.generateResult(
                    ConnectorPluginsResourceTestConnector.class.getName(),
                    resultConfigKeys,
                    configValues,
                    Collections.singletonList("Test")
            );
            configInfosCallback.getValue().onCompletion(null, configInfos);
            return null;
        });

        PowerMock.replayAll();

        // make a request to connector-plugins resource using a valid alias.
        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(
            "ConnectorPluginsResourceTest",
            props
        );
        assertEquals(CONFIG_INFOS.name(), configInfos.name());
        assertEquals(0, configInfos.errorCount());
        assertEquals(CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(new HashSet<>(CONFIG_INFOS.values()), new HashSet<>(configInfos.values()));

        PowerMock.verifyAll();
    }

    @Test
    public void testValidateConfigWithNonExistentName() {
        // make a request to connector-plugins resource using a non-loaded connector with the same
        // simple name but different package.
        String customClassname = "com.custom.package."
            + ConnectorPluginsResourceTestConnector.class.getSimpleName();
        assertThrows(BadRequestException.class, () -> connectorPluginsResource.validateConfigs(customClassname, props));
    }

    @Test
    public void testValidateConfigWithNonExistentAlias() {
        assertThrows(BadRequestException.class, () -> connectorPluginsResource.validateConfigs("ConnectorPluginsTest", props));
    }

    @Test
    public void testListConnectorPlugins() throws Exception {
        expectPlugins();
        Set<ConnectorPluginInfo> connectorPlugins = new HashSet<>(connectorPluginsResource.listConnectorPlugins());
        assertFalse(connectorPlugins.contains(newInfo(Connector.class, "0.0")));
        assertFalse(connectorPlugins.contains(newInfo(SourceConnector.class, "0.0")));
        assertFalse(connectorPlugins.contains(newInfo(SinkConnector.class, "0.0")));
        assertFalse(connectorPlugins.contains(newInfo(VerifiableSourceConnector.class)));
        assertFalse(connectorPlugins.contains(newInfo(VerifiableSinkConnector.class)));
        assertFalse(connectorPlugins.contains(newInfo(MockSourceConnector.class)));
        assertFalse(connectorPlugins.contains(newInfo(MockSinkConnector.class)));
        assertFalse(connectorPlugins.contains(newInfo(MockConnector.class)));
        assertFalse(connectorPlugins.contains(newInfo(SchemaSourceConnector.class)));
        assertTrue(connectorPlugins.contains(newInfo(ConnectorPluginsResourceTestConnector.class)));
        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorPluginsIncludesTypeAndVersionInformation() throws Exception {
        expectPlugins();
        ConnectorPluginInfo sinkInfo = newInfo(TestSinkConnector.class);
        ConnectorPluginInfo sourceInfo =
                newInfo(TestSourceConnector.class);
        ConnectorPluginInfo unknownInfo =
            newInfo(ConnectorPluginsResourceTestConnector.class);
        assertEquals(ConnectorType.SINK, sinkInfo.type());
        assertEquals(ConnectorType.SOURCE, sourceInfo.type());
        assertEquals(ConnectorType.UNKNOWN, unknownInfo.type());
        assertEquals(TestSinkConnector.VERSION, sinkInfo.version());
        assertEquals(TestSourceConnector.VERSION, sourceInfo.version());

        final ObjectMapper objectMapper = new ObjectMapper();
        String serializedSink = objectMapper.writeValueAsString(ConnectorType.SINK);
        String serializedSource = objectMapper.writeValueAsString(ConnectorType.SOURCE);
        String serializedUnknown = objectMapper.writeValueAsString(ConnectorType.UNKNOWN);
        assertTrue(serializedSink.contains("sink"));
        assertTrue(serializedSource.contains("source"));
        assertTrue(serializedUnknown.contains("unknown"));
        assertEquals(
            ConnectorType.SINK,
            objectMapper.readValue(serializedSink, ConnectorType.class)
        );
        assertEquals(
            ConnectorType.SOURCE,
            objectMapper.readValue(serializedSource, ConnectorType.class)
        );
        assertEquals(
            ConnectorType.UNKNOWN,
            objectMapper.readValue(serializedUnknown, ConnectorType.class)
        );
    }

    protected static ConnectorPluginInfo newInfo(Class<? extends Connector> klass, String version)
            throws Exception {
        return new ConnectorPluginInfo(new MockConnectorPluginDesc(klass, version));
    }

    protected static ConnectorPluginInfo newInfo(Class<? extends Connector> klass)
            throws Exception {
        return new ConnectorPluginInfo(new MockConnectorPluginDesc(klass));
    }

    public static class MockPluginClassLoader extends PluginClassLoader {
        public MockPluginClassLoader(URL pluginLocation, URL[] urls, ClassLoader parent) {
            super(pluginLocation, urls, parent);
        }

        public MockPluginClassLoader(URL pluginLocation, URL[] urls) {
            super(pluginLocation, urls);
        }

        @Override
        public String location() {
            return "/tmp/mockpath";
        }
    }

    public static class MockConnectorPluginDesc extends PluginDesc<Connector> {
        public MockConnectorPluginDesc(Class<? extends Connector> klass, String version) {
            super(klass, version, new MockPluginClassLoader(null, new URL[0]));
        }

        public MockConnectorPluginDesc(Class<? extends Connector> klass) throws Exception {
            super(
                    klass,
                    klass.getConstructor().newInstance().version(),
                    new MockPluginClassLoader(null, new URL[0])
            );
        }
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
            return Arrays.asList(1, 2, 3);
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private static class ListRecommender implements Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return Arrays.asList("a", "b", "c");
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }
}
