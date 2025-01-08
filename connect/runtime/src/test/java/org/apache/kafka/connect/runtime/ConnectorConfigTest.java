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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectorConfigTest<R extends ConnectRecord<R>> {

    public static final Plugins MOCK_PLUGINS = new Plugins(new HashMap<>()) {
        @Override
        public Set<PluginDesc<Transformation<?>>> transformations() {
            return Collections.emptySet();
        }
    };

    private static final SinkRecord DUMMY_RECORD = new SinkRecord(null, 0, null, null, null, null, 0L);

    public abstract static class TestConnector extends Connector {
    }

    public static class SimpleTransformation<R extends ConnectRecord<R>> implements Transformation<R>, Versioned  {

        int magicNumber = 0;

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void configure(Map<String, ?> props) {
            magicNumber = Integer.parseInt((String) props.get("magic.number"));
        }

        @Override
        public R apply(R record) {
            return record.newRecord(null, magicNumber, null, null, null, null, 0L);
        }

        @Override
        public void close() {
            magicNumber = 0;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("magic.number", ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Range.atLeast(42), ConfigDef.Importance.HIGH, "");
        }
    }

    @Test
    public void noTransforms() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test
    public void danglingTransformAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "dangler");
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("Not a Transformation"));
    }

    @Test
    public void emptyConnectorName() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "");
        props.put("connector.class", TestConnector.class.getName());
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("String may not be empty"));
    }

    @Test
    public void wrongTransformationType() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", "uninstantiable");
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("Class uninstantiable could not be found"));
    }

    @Test
    public void unconfiguredTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("Missing required configuration \"transforms.a.magic.number\" which"));
    }

    @Test
    public void misconfiguredTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "40");
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("Value must be at least 42"));
    }

    @Test
    public void singleTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        final ConnectorConfig config = new ConnectorConfig(MOCK_PLUGINS, props);
        final List<TransformationStage<SinkRecord>> transformationStages = config.transformationStages();
        assertEquals(1, transformationStages.size());
        final TransformationStage<SinkRecord> stage = transformationStages.get(0);
        assertEquals(SimpleTransformation.class, stage.transformClass());
        assertEquals(42, stage.apply(DUMMY_RECORD).kafkaPartition().intValue());
    }

    @Test
    public void multipleTransformsOneDangling() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a, b");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
    }

    @Test
    public void multipleTransforms() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a, b");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.b.type", SimpleTransformation.class.getName());
        props.put("transforms.b.magic.number", "84");
        final ConnectorConfig config = new ConnectorConfig(MOCK_PLUGINS, props);
        final List<TransformationStage<SinkRecord>> transformationStages = config.transformationStages();
        assertEquals(2, transformationStages.size());
        assertEquals(42, transformationStages.get(0).apply(DUMMY_RECORD).kafkaPartition().intValue());
        assertEquals(84, transformationStages.get(1).apply(DUMMY_RECORD).kafkaPartition().intValue());
    }

    @Test
    public void abstractTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", AbstractTransformation.class.getName());
        ConfigException ex = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(
            ex.getMessage().contains("This class is abstract and cannot be created.")
        );
    }
    @Test
    public void abstractKeyValueTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", AbstractKeyValueTransformation.class.getName());
        ConfigException ex = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(
            ex.getMessage().contains("This class is abstract and cannot be created.")
        );
        assertTrue(
            ex.getMessage().contains(AbstractKeyValueTransformation.Key.class.getName())
        );
        assertTrue(
            ex.getMessage().contains(AbstractKeyValueTransformation.Value.class.getName())
        );
    }

    @Test
    public void wrongPredicateType() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestConnector.class.getName());
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertEquals("Class " + TestConnector.class + " does not implement the Predicate interface", e.getMessage());
    }

    @Test
    public void singleConditionalTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("transforms.a.negate", "true");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        assertTransformationStageWithPredicate(props, true);
    }

    @Test
    public void predicateNegationDefaultsToFalse() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        assertTransformationStageWithPredicate(props, false);
    }

    @Test
    public void abstractPredicate() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", AbstractTestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("This class is abstract and cannot be created"));
    }

    private void assertTransformationStageWithPredicate(Map<String, String> props, boolean expectedNegated) {
        final ConnectorConfig config = new ConnectorConfig(MOCK_PLUGINS, props);
        final List<TransformationStage<SinkRecord>> transformationStages = config.transformationStages();
        assertEquals(1, transformationStages.size());
        TransformationStage<SinkRecord> stage = transformationStages.get(0);

        assertEquals(expectedNegated ? 42 : 0, stage.apply(DUMMY_RECORD).kafkaPartition().intValue());

        SinkRecord matchingRecord = DUMMY_RECORD.newRecord(null, 84, null, null, null, null, 0L);
        assertEquals(expectedNegated ? 84 : 42, stage.apply(matchingRecord).kafkaPartition().intValue());
        assertEquals(SimpleTransformation.class, stage.transformClass());

        stage.close();
    }

    @Test
    public void misconfiguredPredicate() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("transforms.a.negate", "true");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "79");
        try {
            new ConnectorConfig(MOCK_PLUGINS, props);
            fail();
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("Value must be at least 80"));
        }
    }

    @Test
    public void missingPredicateAliasProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        // technically not needed
        //props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test
    public void missingPredicateConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        //props.put("predicates.my-pred.type", TestPredicate.class.getName());
        //props.put("predicates.my-pred.int", "84");
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("Not a Predicate"));
    }

    @Test
    public void negatedButNoPredicate() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.negate", "true");
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(MOCK_PLUGINS, props));
        assertTrue(e.getMessage().contains("there is no config 'transforms.a.predicate' defining a predicate to be negated"));
    }

    public static class TestPredicate<R extends ConnectRecord<R>> implements Predicate<R>  {

        int param;

        public TestPredicate() { }

        @Override
        public ConfigDef config() {
            return new ConfigDef().define("int", ConfigDef.Type.INT, 80, ConfigDef.Range.atLeast(80), ConfigDef.Importance.MEDIUM,
                    "A test parameter");
        }

        @Override
        public boolean test(R record) {
            return record.kafkaPartition() == param;
        }

        @Override
        public void close() {
            param = 0;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            param = Integer.parseInt((String) configs.get("int"));
        }
    }

    public abstract static class AbstractTestPredicate<R extends ConnectRecord<R>> implements Predicate<R>, Versioned {

        @Override
        public String version() {
            return "1.0";
        }

        public AbstractTestPredicate() { }

    }

    public abstract static class AbstractTransformation<R extends ConnectRecord<R>> implements Transformation<R>, Versioned  {

        @Override
        public String version() {
            return "1.0";
        }

    }

    public abstract static class AbstractKeyValueTransformation<R extends ConnectRecord<R>> implements Transformation<R>, Versioned  {
        @Override
        public R apply(R record) {
            return null;
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }


        public static class Key<R extends ConnectRecord<R>> extends AbstractKeyValueTransformation<R> implements Versioned {

            @Override
            public String version() {
                return "1.0";
            }

        }
        public static class Value<R extends ConnectRecord<R>> extends AbstractKeyValueTransformation<R> {

        }
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testEnrichedConfigDef() throws ClassNotFoundException {
        String alias = "hdt";
        String prefix = ConnectorConfig.TRANSFORMS_CONFIG + "." + alias + ".";
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.TRANSFORMS_CONFIG, alias);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());
        props.put(prefix + "type", HasDuplicateConfigTransformation.class.getName());
        Plugins mockPlugins = mock(Plugins.class);
        when(mockPlugins.newPlugin(HasDuplicateConfigTransformation.class.getName(),
                null, (ClassLoader) null)).thenReturn(new HasDuplicateConfigTransformation());
        when(mockPlugins.transformations()).thenReturn(Collections.emptySet());
        ConfigDef def = ConnectorConfig.enrich(mockPlugins, new ConfigDef(), props, false);
        assertEnrichedConfigDef(def, prefix, HasDuplicateConfigTransformation.MUST_EXIST_KEY, ConfigDef.Type.BOOLEAN);
        assertEnrichedConfigDef(def, prefix, TransformationStage.PREDICATE_CONFIG, ConfigDef.Type.STRING);
        assertEnrichedConfigDef(def, prefix, TransformationStage.NEGATE_CONFIG, ConfigDef.Type.BOOLEAN);
    }

    private static void assertEnrichedConfigDef(ConfigDef def, String prefix, String keyName, ConfigDef.Type expectedType) {
        assertNull(def.configKeys().get(keyName));
        ConfigDef.ConfigKey configKey = def.configKeys().get(prefix + keyName);
        assertNotNull(configKey, prefix + keyName + "' config must be present");
        assertEquals(expectedType, configKey.type, prefix + keyName + "' config should be a " + expectedType);
    }

    public static class HasDuplicateConfigTransformation<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
        private static final String MUST_EXIST_KEY = "must.exist.key";
        private static final ConfigDef CONFIG_DEF = new ConfigDef()
                // this configDef is duplicate. It should be removed automatically so as to avoid duplicate config error.
                .define(TransformationStage.PREDICATE_CONFIG, ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "fake")
                // this configDef is duplicate. It should be removed automatically so as to avoid duplicate config error.
                .define(TransformationStage.NEGATE_CONFIG, ConfigDef.Type.INT, 123, ConfigDef.Importance.MEDIUM, "fake")
                // this configDef should appear if above duplicate configDef is removed without any error
                .define(MUST_EXIST_KEY, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, "this key must exist");

        @Override
        public R apply(R record) {
            return record;
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }
}
