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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.converters.LongConverter;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.SampleSinkConnector;
import org.apache.kafka.connect.runtime.SampleSourceConnector;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoaderTest;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.tools.SchemaSourceConnector;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.apache.kafka.connect.transforms.predicates.RecordIsTombstone;
import org.apache.kafka.connect.util.Callback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.BadRequestException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ConnectorPluginsResourceTest {

    private static final Map<String, String> PROPS;
    private static final Map<String, String> PARTIAL_PROPS = new HashMap<>();
    static {
        PARTIAL_PROPS.put("name", "test");
        PARTIAL_PROPS.put("test.string.config", "testString");
        PARTIAL_PROPS.put("test.int.config", "1");
        PARTIAL_PROPS.put("test.list.config", "a,b");

        PROPS = new HashMap<>(PARTIAL_PROPS);
        PROPS.put("connector.class", ConnectorPluginsResourceTestConnector.class.getSimpleName());
    }

    private static final ConfigInfos CONFIG_INFOS;
    private static final ConfigInfos PARTIAL_CONFIG_INFOS;
    private static final int ERROR_COUNT = 0;
    private static final int PARTIAL_CONFIG_ERROR_COUNT = 1;
    private static final Set<PluginDesc<?>> SINK_CONNECTOR_PLUGINS = new TreeSet<>();
    private static final Set<PluginDesc<?>> SOURCE_CONNECTOR_PLUGINS = new TreeSet<>();
    private static final Set<PluginDesc<?>> CONVERTER_PLUGINS = new TreeSet<>();
    private static final Set<PluginDesc<?>> HEADER_CONVERTER_PLUGINS = new TreeSet<>();
    private static final Set<PluginDesc<?>> TRANSFORMATION_PLUGINS = new TreeSet<>();
    private static final Set<PluginDesc<?>> PREDICATE_PLUGINS = new TreeSet<>();

    static {
        try {
            ClassLoader classLoader = ConnectorPluginsResourceTest.class.getClassLoader();
            ClassLoader pluginClassLoader = new PluginClassLoader(DelegatingClassLoaderTest.ARBITRARY_URL, new URL[]{}, classLoader);
            String appVersion = AppInfoParser.getVersion();
            SINK_CONNECTOR_PLUGINS.add(new PluginDesc<>(VerifiableSinkConnector.class, appVersion, PluginType.SINK, classLoader));
            SINK_CONNECTOR_PLUGINS.add(new PluginDesc<>(MockSinkConnector.class, appVersion, PluginType.SINK, classLoader));

            SOURCE_CONNECTOR_PLUGINS.add(new PluginDesc<>(VerifiableSourceConnector.class, appVersion, PluginType.SOURCE, classLoader));
            SOURCE_CONNECTOR_PLUGINS.add(new PluginDesc<>(MockSourceConnector.class, appVersion, PluginType.SOURCE, classLoader));
            SOURCE_CONNECTOR_PLUGINS.add(new PluginDesc<>(SchemaSourceConnector.class, appVersion, PluginType.SOURCE, classLoader));
            SOURCE_CONNECTOR_PLUGINS.add(new PluginDesc<>(ConnectorPluginsResourceTestConnector.class, appVersion, PluginType.SOURCE, classLoader));

            CONVERTER_PLUGINS.add(new PluginDesc<>(StringConverter.class, appVersion, PluginType.CONVERTER, classLoader));
            CONVERTER_PLUGINS.add(new PluginDesc<>(LongConverter.class, appVersion, PluginType.CONVERTER, classLoader));

            // Same class, version, and type, but loaded from a different classloader
            CONVERTER_PLUGINS.add(new PluginDesc<>(StringConverter.class, appVersion, PluginType.CONVERTER, pluginClassLoader));
            CONVERTER_PLUGINS.add(new PluginDesc<>(LongConverter.class, appVersion, PluginType.CONVERTER, pluginClassLoader));

            HEADER_CONVERTER_PLUGINS.add(new PluginDesc<>(StringConverter.class, appVersion, PluginType.HEADER_CONVERTER, classLoader));
            HEADER_CONVERTER_PLUGINS.add(new PluginDesc<>(LongConverter.class, appVersion, PluginType.HEADER_CONVERTER, classLoader));

            TRANSFORMATION_PLUGINS.add(new PluginDesc<>(RegexRouter.class, appVersion, PluginType.TRANSFORMATION, classLoader));
            TRANSFORMATION_PLUGINS.add(new PluginDesc<>(TimestampConverter.Key.class, appVersion, PluginType.TRANSFORMATION, classLoader));

            PREDICATE_PLUGINS.add(new PluginDesc<>(HasHeaderKey.class, appVersion, PluginType.PREDICATE, classLoader));
            PREDICATE_PLUGINS.add(new PluginDesc<>(RecordIsTombstone.class, appVersion, PluginType.PREDICATE, classLoader));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed setting up plugins");
        }
    }

    static {
        List<ConfigInfo> configs = new LinkedList<>();
        List<ConfigInfo> partialConfigs = new LinkedList<>();

        ConfigDef connectorConfigDef = ConnectorConfig.configDef();
        List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(PROPS);
        List<ConfigValue> partialConnectorConfigValues = connectorConfigDef.validate(PARTIAL_PROPS);
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
        configValueInfo = new ConfigValueInfo("test.int.config", "1", asList("1", "2", "3"), Collections.emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.string.config.default", "STRING", false, "", "LOW", "Test configuration with default value.", null, -1, "NONE", "test.string.config.default", Collections.emptyList());
        configValueInfo = new ConfigValueInfo("test.string.config.default", "", Collections.emptyList(), Collections.emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        configKeyInfo = new ConfigKeyInfo("test.list.config", "LIST", true, null, "HIGH", "Test configuration for list type.", "Test", 2, "LONG", "test.list.config", Collections.emptyList());
        configValueInfo = new ConfigValueInfo("test.list.config", "a,b", asList("a", "b", "c"), Collections.emptyList(), true);
        configInfo = new ConfigInfo(configKeyInfo, configValueInfo);
        configs.add(configInfo);
        partialConfigs.add(configInfo);

        CONFIG_INFOS = new ConfigInfos(ConnectorPluginsResourceTestConnector.class.getName(), ERROR_COUNT, Collections.singletonList("Test"), configs);
        PARTIAL_CONFIG_INFOS = new ConfigInfos(ConnectorPluginsResourceTestConnector.class.getName(), PARTIAL_CONFIG_ERROR_COUNT, Collections.singletonList("Test"), partialConfigs);
    }

    private final Herder herder = mock(DistributedHerder.class);
    private final Plugins plugins = mock(Plugins.class);
    private ConnectorPluginsResource connectorPluginsResource;

    @Before
    public void setUp() throws Exception {
        doReturn(plugins).when(herder).plugins();
        doReturn(SINK_CONNECTOR_PLUGINS).when(plugins).sinkConnectors();
        doReturn(SOURCE_CONNECTOR_PLUGINS).when(plugins).sourceConnectors();
        doReturn(CONVERTER_PLUGINS).when(plugins).converters();
        doReturn(HEADER_CONVERTER_PLUGINS).when(plugins).headerConverters();
        doReturn(TRANSFORMATION_PLUGINS).when(plugins).transformations();
        doReturn(PREDICATE_PLUGINS).when(plugins).predicates();
        connectorPluginsResource = new ConnectorPluginsResource(herder);
    }

    @Test
    public void testValidateConfigWithSingleErrorDueToMissingConnectorClassname() throws Throwable {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Callback<ConfigInfos>> configInfosCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            ConfigDef connectorConfigDef = ConnectorConfig.configDef();
            List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(PARTIAL_PROPS);

            Connector connector = new ConnectorPluginsResourceTestConnector();
            Config config = connector.validate(PARTIAL_PROPS);
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
        }).when(herder).validateConnectorConfig(eq(PARTIAL_PROPS), configInfosCallback.capture(), anyBoolean());

        // This call to validateConfigs does not throw a BadRequestException because we've mocked
        // validateConnectorConfig.
        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(
            ConnectorPluginsResourceTestConnector.class.getSimpleName(),
            PARTIAL_PROPS
        );
        assertEquals(PARTIAL_CONFIG_INFOS.name(), configInfos.name());
        assertEquals(PARTIAL_CONFIG_INFOS.errorCount(), configInfos.errorCount());
        assertEquals(PARTIAL_CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(
            new HashSet<>(PARTIAL_CONFIG_INFOS.values()),
            new HashSet<>(configInfos.values())
        );
        verify(herder).validateConnectorConfig(eq(PARTIAL_PROPS), any(), anyBoolean());
    }

    @Test
    public void testValidateConfigWithSimpleName() throws Throwable {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Callback<ConfigInfos>> configInfosCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            ConfigDef connectorConfigDef = ConnectorConfig.configDef();
            List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(PROPS);

            Connector connector = new ConnectorPluginsResourceTestConnector();
            Config config = connector.validate(PROPS);
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
        }).when(herder).validateConnectorConfig(eq(PROPS), configInfosCallback.capture(), anyBoolean());

        // make a request to connector-plugins resource using just the simple class name.
        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(
            ConnectorPluginsResourceTestConnector.class.getSimpleName(),
            PROPS
        );
        assertEquals(CONFIG_INFOS.name(), configInfos.name());
        assertEquals(0, configInfos.errorCount());
        assertEquals(CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(new HashSet<>(CONFIG_INFOS.values()), new HashSet<>(configInfos.values()));
        verify(herder).validateConnectorConfig(eq(PROPS), any(), anyBoolean());
    }

    @Test
    public void testValidateConfigWithAlias() throws Throwable {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Callback<ConfigInfos>> configInfosCallback = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            ConfigDef connectorConfigDef = ConnectorConfig.configDef();
            List<ConfigValue> connectorConfigValues = connectorConfigDef.validate(PROPS);

            Connector connector = new ConnectorPluginsResourceTestConnector();
            Config config = connector.validate(PROPS);
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
        }).when(herder).validateConnectorConfig(eq(PROPS), configInfosCallback.capture(), anyBoolean());

        // make a request to connector-plugins resource using a valid alias.
        ConfigInfos configInfos = connectorPluginsResource.validateConfigs(
            "ConnectorPluginsResourceTest",
            PROPS
        );
        assertEquals(CONFIG_INFOS.name(), configInfos.name());
        assertEquals(0, configInfos.errorCount());
        assertEquals(CONFIG_INFOS.groups(), configInfos.groups());
        assertEquals(new HashSet<>(CONFIG_INFOS.values()), new HashSet<>(configInfos.values()));
        verify(herder).validateConnectorConfig(eq(PROPS), any(), anyBoolean());
    }

    @Test
    public void testValidateConfigWithNonExistentName() {
        // make a request to connector-plugins resource using a non-loaded connector with the same
        // simple name but different package.
        String customClassname = "com.custom.package."
            + ConnectorPluginsResourceTestConnector.class.getSimpleName();
        assertThrows(BadRequestException.class, () -> connectorPluginsResource.validateConfigs(customClassname, PROPS));
    }

    @Test
    public void testValidateConfigWithNonExistentAlias() {
        assertThrows(BadRequestException.class, () -> connectorPluginsResource.validateConfigs("ConnectorPluginsTest", PROPS));
    }

    @Test
    public void testListConnectorPlugins() {
        Set<PluginInfo> expectedConnectorPlugins = Stream.of(SINK_CONNECTOR_PLUGINS, SOURCE_CONNECTOR_PLUGINS)
                .flatMap(Collection::stream)
                .map(PluginInfo::new)
                .collect(Collectors.toSet());
        Set<PluginInfo> actualConnectorPlugins = new HashSet<>(connectorPluginsResource.listConnectorPlugins(true));
        assertEquals(expectedConnectorPlugins, actualConnectorPlugins);
        verify(herder, atLeastOnce()).plugins();
    }

    @Test
    public void testConnectorPluginsIncludesClassTypeAndVersionInformation() throws Exception {
        ClassLoader classLoader = ConnectorPluginsResourceTest.class.getClassLoader();
        PluginInfo sinkInfo = new PluginInfo(new PluginDesc<>(SampleSinkConnector.class, SampleSinkConnector.VERSION, PluginType.SINK, classLoader));
        PluginInfo sourceInfo = new PluginInfo(new PluginDesc<>(SampleSourceConnector.class, SampleSourceConnector.VERSION, PluginType.SOURCE, classLoader));
        assertEquals(PluginType.SINK.toString(), sinkInfo.type());
        assertEquals(PluginType.SOURCE.toString(), sourceInfo.type());
        assertEquals(SampleSinkConnector.VERSION, sinkInfo.version());
        assertEquals(SampleSourceConnector.VERSION, sourceInfo.version());
        assertEquals(SampleSinkConnector.class.getName(), sinkInfo.className());
        assertEquals(SampleSourceConnector.class.getName(), sourceInfo.className());

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

    @Test
    public void testListAllPlugins() {
        List<PluginInfo> expectedConnectorPlugins = Stream.of(
                        SINK_CONNECTOR_PLUGINS,
                        SOURCE_CONNECTOR_PLUGINS,
                        CONVERTER_PLUGINS,
                        HEADER_CONVERTER_PLUGINS,
                        TRANSFORMATION_PLUGINS,
                        PREDICATE_PLUGINS
                ).flatMap(Collection::stream)
                .map(PluginInfo::new)
                .distinct()
                .collect(Collectors.toList());
        List<PluginInfo> actualConnectorPlugins = new ArrayList<>(connectorPluginsResource.listConnectorPlugins(false));
        Comparator<PluginInfo> compare = Comparator.comparing(PluginInfo::className)
                .thenComparing(PluginInfo::type)
                .thenComparing(PluginInfo::version);
        actualConnectorPlugins.sort(compare);
        expectedConnectorPlugins.sort(compare);
        assertEquals(expectedConnectorPlugins, actualConnectorPlugins);
        verify(herder, atLeastOnce()).plugins();
    }

    @Test
    public void testGetConnectorConfigDef() {
        String connName = ConnectorPluginsResourceTestConnector.class.getName();
        when(herder.connectorPluginConfig(eq(connName))).thenAnswer(answer -> {
            List<ConfigKeyInfo> results = new ArrayList<>();
            for (ConfigDef.ConfigKey configKey : ConnectorPluginsResourceTestConnector.CONFIG_DEF.configKeys().values()) {
                results.add(AbstractHerder.convertConfigKey(configKey));
            }
            return results;
        });
        List<ConfigKeyInfo> connectorConfigDef = connectorPluginsResource.getConnectorConfigDef(connName);
        assertEquals(ConnectorPluginsResourceTestConnector.CONFIG_DEF.names().size(), connectorConfigDef.size());
        for (String config : ConnectorPluginsResourceTestConnector.CONFIG_DEF.names()) {
            Optional<ConfigKeyInfo> cki = connectorConfigDef.stream().filter(c -> c.name().equals(config)).findFirst();
            assertTrue(cki.isPresent());
        }
    }

    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class ConnectorPluginsResourceTestConnector extends SourceConnector {

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
            return AppInfoParser.getVersion();
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
            return asList(1, 2, 3);
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private static class ListRecommender implements Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return asList("a", "b", "c");
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }
}
