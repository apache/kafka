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
package org.apache.kafka.common.config;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metrics.FakeMetricsReporter;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.config.provider.MockVaultConfigProvider;
import org.apache.kafka.common.config.provider.MockFileConfigProvider;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class AbstractConfigTest {

    @Test
    public void testConfiguredInstances() {
        testValidInputs("");
        testValidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter");
        testValidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter, org.apache.kafka.common.metrics.FakeMetricsReporter");
        testInvalidInputs(",");
        testInvalidInputs("org.apache.kafka.clients.producer.unknown-metrics-reporter");
        testInvalidInputs("test1,test2");
        testInvalidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter,");
    }

    @Test
    public void testEmptyList() {
        AbstractConfig conf;
        ConfigDef configDef = new ConfigDef().define("a", Type.LIST, "", new ConfigDef.NonNullValidator(), Importance.HIGH, "doc");

        conf = new AbstractConfig(configDef, Collections.emptyMap());
        assertEquals(Collections.emptyList(), conf.getList("a"));

        conf = new AbstractConfig(configDef, Collections.singletonMap("a", ""));
        assertEquals(Collections.emptyList(), conf.getList("a"));

        conf = new AbstractConfig(configDef, Collections.singletonMap("a", "b,c,d"));
        assertEquals(Arrays.asList("b", "c", "d"), conf.getList("a"));
    }

    @Test
    public void testOriginalsWithPrefix() {
        Properties props = new Properties();
        props.put("foo.bar", "abc");
        props.put("setting", "def");
        TestConfig config = new TestConfig(props);
        Map<String, Object> originalsWithPrefix = config.originalsWithPrefix("foo.");

        assertTrue(config.unused().contains("foo.bar"));
        originalsWithPrefix.get("bar");
        assertFalse(config.unused().contains("foo.bar"));

        Map<String, Object> expected = new HashMap<>();
        expected.put("bar", "abc");
        assertEquals(expected, originalsWithPrefix);
    }

    @Test
    public void testValuesWithPrefixOverride() {
        String prefix = "prefix.";
        Properties props = new Properties();
        props.put("sasl.mechanism", "PLAIN");
        props.put("prefix.sasl.mechanism", "GSSAPI");
        props.put("prefix.sasl.kerberos.kinit.cmd", "/usr/bin/kinit2");
        props.put("prefix.ssl.truststore.location", "my location");
        props.put("sasl.kerberos.service.name", "service name");
        props.put("ssl.keymanager.algorithm", "algorithm");
        TestSecurityConfig config = new TestSecurityConfig(props);
        Map<String, Object> valuesWithPrefixOverride = config.valuesWithPrefixOverride(prefix);

        // prefix overrides global
        assertTrue(config.unused().contains("prefix.sasl.mechanism"));
        assertTrue(config.unused().contains("sasl.mechanism"));
        assertEquals("GSSAPI", valuesWithPrefixOverride.get("sasl.mechanism"));
        assertFalse(config.unused().contains("sasl.mechanism"));
        assertFalse(config.unused().contains("prefix.sasl.mechanism"));

        // prefix overrides default
        assertTrue(config.unused().contains("prefix.sasl.kerberos.kinit.cmd"));
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"));
        assertEquals("/usr/bin/kinit2", valuesWithPrefixOverride.get("sasl.kerberos.kinit.cmd"));
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"));
        assertFalse(config.unused().contains("prefix.sasl.kerberos.kinit.cmd"));

        // prefix override with no default
        assertTrue(config.unused().contains("prefix.ssl.truststore.location"));
        assertFalse(config.unused().contains("ssl.truststore.location"));
        assertEquals("my location", valuesWithPrefixOverride.get("ssl.truststore.location"));
        assertFalse(config.unused().contains("ssl.truststore.location"));
        assertFalse(config.unused().contains("prefix.ssl.truststore.location"));

        // global overrides default
        assertTrue(config.unused().contains("ssl.keymanager.algorithm"));
        assertEquals("algorithm", valuesWithPrefixOverride.get("ssl.keymanager.algorithm"));
        assertFalse(config.unused().contains("ssl.keymanager.algorithm"));

        // global with no default
        assertTrue(config.unused().contains("sasl.kerberos.service.name"));
        assertEquals("service name", valuesWithPrefixOverride.get("sasl.kerberos.service.name"));
        assertFalse(config.unused().contains("sasl.kerberos.service.name"));

        // unset with default
        assertFalse(config.unused().contains("sasl.kerberos.min.time.before.relogin"));
        assertEquals(SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
                valuesWithPrefixOverride.get("sasl.kerberos.min.time.before.relogin"));
        assertFalse(config.unused().contains("sasl.kerberos.min.time.before.relogin"));

        // unset with no default
        assertFalse(config.unused().contains("ssl.key.password"));
        assertNull(valuesWithPrefixOverride.get("ssl.key.password"));
        assertFalse(config.unused().contains("ssl.key.password"));
    }

    @Test
    public void testValuesWithSecondaryPrefix() {
        String prefix = "listener.name.listener1.";
        Password saslJaasConfig1 =  new Password("test.myLoginModule1 required;");
        Password saslJaasConfig2 =  new Password("test.myLoginModule2 required;");
        Password saslJaasConfig3 =  new Password("test.myLoginModule3 required;");
        Properties props = new Properties();
        props.put("listener.name.listener1.test-mechanism.sasl.jaas.config", saslJaasConfig1.value());
        props.put("test-mechanism.sasl.jaas.config", saslJaasConfig2.value());
        props.put("sasl.jaas.config", saslJaasConfig3.value());
        props.put("listener.name.listener1.gssapi.sasl.kerberos.kinit.cmd", "/usr/bin/kinit2");
        props.put("listener.name.listener1.gssapi.sasl.kerberos.service.name", "testkafka");
        props.put("listener.name.listener1.gssapi.sasl.kerberos.min.time.before.relogin", "60000");
        props.put("ssl.provider", "TEST");
        TestSecurityConfig config = new TestSecurityConfig(props);
        Map<String, Object> valuesWithPrefixOverride = config.valuesWithPrefixOverride(prefix);

        // prefix with mechanism overrides global
        assertTrue(config.unused().contains("listener.name.listener1.test-mechanism.sasl.jaas.config"));
        assertTrue(config.unused().contains("test-mechanism.sasl.jaas.config"));
        assertEquals(saslJaasConfig1, valuesWithPrefixOverride.get("test-mechanism.sasl.jaas.config"));
        assertEquals(saslJaasConfig3, valuesWithPrefixOverride.get("sasl.jaas.config"));
        assertFalse(config.unused().contains("listener.name.listener1.test-mechanism.sasl.jaas.config"));
        assertFalse(config.unused().contains("test-mechanism.sasl.jaas.config"));
        assertFalse(config.unused().contains("sasl.jaas.config"));

        // prefix with mechanism overrides default
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"));
        assertTrue(config.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.kinit.cmd"));
        assertFalse(config.unused().contains("gssapi.sasl.kerberos.kinit.cmd"));
        assertFalse(config.unused().contains("sasl.kerberos.kinit.cmd"));
        assertEquals("/usr/bin/kinit2", valuesWithPrefixOverride.get("gssapi.sasl.kerberos.kinit.cmd"));
        assertFalse(config.unused().contains("listener.name.listener1.sasl.kerberos.kinit.cmd"));

        // prefix override for mechanism with no default
        assertFalse(config.unused().contains("sasl.kerberos.service.name"));
        assertTrue(config.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"));
        assertFalse(config.unused().contains("gssapi.sasl.kerberos.service.name"));
        assertFalse(config.unused().contains("sasl.kerberos.service.name"));
        assertEquals("testkafka", valuesWithPrefixOverride.get("gssapi.sasl.kerberos.service.name"));
        assertFalse(config.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"));

        // unset with no default
        assertTrue(config.unused().contains("ssl.provider"));
        assertNull(valuesWithPrefixOverride.get("gssapi.ssl.provider"));
        assertTrue(config.unused().contains("ssl.provider"));
    }

    @Test
    public void testValuesWithPrefixAllOrNothing() {
        String prefix1 = "prefix1.";
        String prefix2 = "prefix2.";
        Properties props = new Properties();
        props.put("sasl.mechanism", "PLAIN");
        props.put("prefix1.sasl.mechanism", "GSSAPI");
        props.put("prefix1.sasl.kerberos.kinit.cmd", "/usr/bin/kinit2");
        props.put("prefix1.ssl.truststore.location", "my location");
        props.put("sasl.kerberos.service.name", "service name");
        props.put("ssl.keymanager.algorithm", "algorithm");
        TestSecurityConfig config = new TestSecurityConfig(props);
        Map<String, Object> valuesWithPrefixAllOrNothing1 = config.valuesWithPrefixAllOrNothing(prefix1);

        // All prefixed values are there
        assertEquals("GSSAPI", valuesWithPrefixAllOrNothing1.get("sasl.mechanism"));
        assertEquals("/usr/bin/kinit2", valuesWithPrefixAllOrNothing1.get("sasl.kerberos.kinit.cmd"));
        assertEquals("my location", valuesWithPrefixAllOrNothing1.get("ssl.truststore.location"));

        // Non-prefixed values are missing
        assertFalse(valuesWithPrefixAllOrNothing1.containsKey("sasl.kerberos.service.name"));
        assertFalse(valuesWithPrefixAllOrNothing1.containsKey("ssl.keymanager.algorithm"));

        Map<String, Object> valuesWithPrefixAllOrNothing2 = config.valuesWithPrefixAllOrNothing(prefix2);
        assertTrue(valuesWithPrefixAllOrNothing2.containsKey("sasl.kerberos.service.name"));
        assertTrue(valuesWithPrefixAllOrNothing2.containsKey("ssl.keymanager.algorithm"));
    }

    @Test
    public void testUnusedConfigs() {
        Properties props = new Properties();
        String configValue = "org.apache.kafka.common.config.AbstractConfigTest$ConfiguredFakeMetricsReporter";
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        props.put(ConfiguredFakeMetricsReporter.EXTRA_CONFIG, "my_value");
        TestConfig config = new TestConfig(props);

        assertTrue(ConfiguredFakeMetricsReporter.EXTRA_CONFIG + " should be marked unused before getConfiguredInstances is called",
            config.unused().contains(ConfiguredFakeMetricsReporter.EXTRA_CONFIG));

        config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        assertFalse(ConfiguredFakeMetricsReporter.EXTRA_CONFIG + " should be marked as used",
            config.unused().contains(ConfiguredFakeMetricsReporter.EXTRA_CONFIG));
    }

    private void testValidInputs(String configValue) {
        Properties props = new Properties();
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        TestConfig config = new TestConfig(props);
        try {
            config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        } catch (ConfigException e) {
            fail("No exceptions are expected here, valid props are :" + props);
        }
    }

    private void testInvalidInputs(String configValue) {
        Properties props = new Properties();
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        TestConfig config = new TestConfig(props);
        try {
            config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
            fail("Expected a config exception due to invalid props :" + props);
        } catch (KafkaException e) {
            // this is good
        }
    }

    @Test
    public void testClassConfigs() {
        class RestrictedClassLoader extends ClassLoader {
            public RestrictedClassLoader() {
                super(null);
            }
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                if (name.equals(ClassTestConfig.DEFAULT_CLASS.getName()) || name.equals(ClassTestConfig.RESTRICTED_CLASS.getName()))
                    throw new ClassNotFoundException();
                else
                    return ClassTestConfig.class.getClassLoader().loadClass(name);
            }
        }

        ClassLoader restrictedClassLoader = new RestrictedClassLoader();
        ClassLoader defaultClassLoader = AbstractConfig.class.getClassLoader();

        // Test default classloading where all classes are visible to thread context classloader
        Thread.currentThread().setContextClassLoader(defaultClassLoader);
        ClassTestConfig testConfig = new ClassTestConfig();
        testConfig.checkInstances(ClassTestConfig.DEFAULT_CLASS, ClassTestConfig.DEFAULT_CLASS);

        // Test default classloading where default classes are not visible to thread context classloader
        // Static classloading is used for default classes, so instance creation should succeed.
        Thread.currentThread().setContextClassLoader(restrictedClassLoader);
        testConfig = new ClassTestConfig();
        testConfig.checkInstances(ClassTestConfig.DEFAULT_CLASS, ClassTestConfig.DEFAULT_CLASS);

        // Test class overrides with names or classes where all classes are visible to thread context classloader
        Thread.currentThread().setContextClassLoader(defaultClassLoader);
        ClassTestConfig.testOverrides();

        // Test class overrides with names or classes where all classes are visible to Kafka classloader, context classloader is null
        Thread.currentThread().setContextClassLoader(null);
        ClassTestConfig.testOverrides();

        // Test class overrides where some classes are not visible to thread context classloader
        Thread.currentThread().setContextClassLoader(restrictedClassLoader);
        // Properties specified as classes should succeed
        testConfig = new ClassTestConfig(ClassTestConfig.RESTRICTED_CLASS, Collections.singletonList(ClassTestConfig.RESTRICTED_CLASS));
        testConfig.checkInstances(ClassTestConfig.RESTRICTED_CLASS, ClassTestConfig.RESTRICTED_CLASS);
        testConfig = new ClassTestConfig(ClassTestConfig.RESTRICTED_CLASS, Arrays.asList(ClassTestConfig.VISIBLE_CLASS, ClassTestConfig.RESTRICTED_CLASS));
        testConfig.checkInstances(ClassTestConfig.RESTRICTED_CLASS, ClassTestConfig.VISIBLE_CLASS, ClassTestConfig.RESTRICTED_CLASS);
        // Properties specified as classNames should fail to load classes
        try {
            new ClassTestConfig(ClassTestConfig.RESTRICTED_CLASS.getName(), null);
            fail("Config created with class property that cannot be loaded");
        } catch (ConfigException e) {
            // Expected Exception
        }
        try {
            testConfig = new ClassTestConfig(null, Arrays.asList(ClassTestConfig.VISIBLE_CLASS.getName(), ClassTestConfig.RESTRICTED_CLASS.getName()));
            testConfig.getConfiguredInstances("list.prop", MetricsReporter.class);
            fail("Should have failed to load class");
        } catch (KafkaException e) {
            // Expected Exception
        }
        try {
            testConfig = new ClassTestConfig(null, ClassTestConfig.VISIBLE_CLASS.getName() + "," + ClassTestConfig.RESTRICTED_CLASS.getName());
            testConfig.getConfiguredInstances("list.prop", MetricsReporter.class);
            fail("Should have failed to load class");
        } catch (KafkaException e) {
            // Expected Exception
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, ?> convertPropertiesToMap(Map<?, ?> props) {
        for (Map.Entry<?, ?> entry : props.entrySet()) {
            if (!(entry.getKey() instanceof String))
                throw new ConfigException(entry.getKey().toString(), entry.getValue(),
                    "Key must be a string.");
        }
        return (Map<String, ?>) props;
    }

    @Test
    public void testOriginalWithOverrides() {
        Properties props = new Properties();
        props.put("config.providers", "file");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props);
        assertEquals(config.originals().get("config.providers"), "file");
        assertEquals(config.originals(Collections.singletonMap("config.providers", "file2")).get("config.providers"), "file2");
    }

    @Test
    public void testOriginalsWithConfigProvidersProps() {
        Properties props = new Properties();

        // Test Case: Valid Test Case for ConfigProviders as part of config.properties
        props.put("config.providers", "file");
        props.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        String id = UUID.randomUUID().toString();
        props.put("config.providers.file.param.testId", id);
        props.put("prefix.ssl.truststore.location.number", 5);
        props.put("sasl.kerberos.service.name", "service name");
        props.put("sasl.kerberos.key", "${file:/usr/kerberos:key}");
        props.put("sasl.kerberos.password", "${file:/usr/kerberos:password}");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props);
        assertEquals("testKey", config.originals().get("sasl.kerberos.key"));
        assertEquals("randomPassword", config.originals().get("sasl.kerberos.password"));
        assertEquals(5, config.originals().get("prefix.ssl.truststore.location.number"));
        assertEquals("service name", config.originals().get("sasl.kerberos.service.name"));
        MockFileConfigProvider.assertClosed(id);
    }

    @Test
    public void testConfigProvidersPropsAsParam() {
        // Test Case: Valid Test Case for ConfigProviders as a separate variable
        Properties providers = new Properties();
        providers.put("config.providers", "file");
        providers.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        String id = UUID.randomUUID().toString();
        providers.put("config.providers.file.param.testId", id);
        Properties props = new Properties();
        props.put("sasl.kerberos.key", "${file:/usr/kerberos:key}");
        props.put("sasl.kerberos.password", "${file:/usr/kerberos:password}");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props, convertPropertiesToMap(providers));
        assertEquals("testKey", config.originals().get("sasl.kerberos.key"));
        assertEquals("randomPassword", config.originals().get("sasl.kerberos.password"));
        MockFileConfigProvider.assertClosed(id);
    }

    @Test
    public void testImmutableOriginalsWithConfigProvidersProps() {
        // Test Case: Valid Test Case for ConfigProviders as a separate variable
        Properties providers = new Properties();
        providers.put("config.providers", "file");
        providers.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        String id = UUID.randomUUID().toString();
        providers.put("config.providers.file.param.testId", id);
        Properties props = new Properties();
        props.put("sasl.kerberos.key", "${file:/usr/kerberos:key}");
        Map<?, ?> immutableMap = Collections.unmodifiableMap(props);
        Map<String, ?> provMap = convertPropertiesToMap(providers);
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(immutableMap, provMap);
        assertEquals("testKey", config.originals().get("sasl.kerberos.key"));
        MockFileConfigProvider.assertClosed(id);
    }

    @Test
    public void testAutoConfigResolutionWithMultipleConfigProviders() {
        // Test Case: Valid Test Case With Multiple ConfigProviders as a separate variable
        Properties providers = new Properties();
        providers.put("config.providers", "file,vault");
        providers.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        String id = UUID.randomUUID().toString();
        providers.put("config.providers.file.param.testId", id);
        providers.put("config.providers.vault.class", MockVaultConfigProvider.class.getName());
        Properties props = new Properties();
        props.put("sasl.kerberos.key", "${file:/usr/kerberos:key}");
        props.put("sasl.kerberos.password", "${file:/usr/kerberos:password}");
        props.put("sasl.truststore.key", "${vault:/usr/truststore:truststoreKey}");
        props.put("sasl.truststore.password", "${vault:/usr/truststore:truststorePassword}");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props, convertPropertiesToMap(providers));
        assertEquals("testKey", config.originals().get("sasl.kerberos.key"));
        assertEquals("randomPassword", config.originals().get("sasl.kerberos.password"));
        assertEquals("testTruststoreKey", config.originals().get("sasl.truststore.key"));
        assertEquals("randomtruststorePassword", config.originals().get("sasl.truststore.password"));
        MockFileConfigProvider.assertClosed(id);
    }

    @Test
    public void testAutoConfigResolutionWithInvalidConfigProviderClass() {
        // Test Case: Invalid class for Config Provider
        Properties props = new Properties();
        props.put("config.providers", "file");
        props.put("config.providers.file.class",
            "org.apache.kafka.common.config.provider.InvalidConfigProvider");
        props.put("testKey", "${test:/foo/bar/testpath:testKey}");
        try {
            new TestIndirectConfigResolution(props);
            fail("Expected a config exception due to invalid props :" + props);
        } catch (KafkaException e) {
            // this is good
        }
    }

    @Test
    public void testAutoConfigResolutionWithMissingConfigProvider() {
        // Test Case: Config Provider for a variable missing in config file.
        Properties props = new Properties();
        props.put("testKey", "${test:/foo/bar/testpath:testKey}");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props);
        assertEquals("${test:/foo/bar/testpath:testKey}", config.originals().get("testKey"));
    }

    @Test
    public void testAutoConfigResolutionWithMissingConfigKey() {
        // Test Case: Config Provider fails to resolve the config (key not present)
        Properties props = new Properties();
        props.put("config.providers", "test");
        props.put("config.providers.test.class", MockFileConfigProvider.class.getName());
        String id = UUID.randomUUID().toString();
        props.put("config.providers.test.param.testId", id);
        props.put("random", "${test:/foo/bar/testpath:random}");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props);
        assertEquals("${test:/foo/bar/testpath:random}", config.originals().get("random"));
        MockFileConfigProvider.assertClosed(id);
    }

    @Test
    public void testAutoConfigResolutionWithDuplicateConfigProvider() {
        // Test Case: If ConfigProvider is provided in both originals and provider. Only the ones in provider should be used.
        Properties providers = new Properties();
        providers.put("config.providers", "test");
        providers.put("config.providers.test.class", MockVaultConfigProvider.class.getName());

        Properties props = new Properties();
        props.put("sasl.kerberos.key", "${file:/usr/kerberos:key}");
        props.put("config.providers", "file");
        props.put("config.providers.file.class", MockVaultConfigProvider.class.getName());

        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props, convertPropertiesToMap(providers));
        assertEquals("${file:/usr/kerberos:key}", config.originals().get("sasl.kerberos.key"));
    }

    @Test
    public void testConfigProviderConfigurationWithConfigParams() {
        // Test Case: Valid Test Case With Multiple ConfigProviders as a separate variable
        Properties providers = new Properties();
        providers.put("config.providers", "vault");
        providers.put("config.providers.vault.class", MockVaultConfigProvider.class.getName());
        providers.put("config.providers.vault.param.key", "randomKey");
        providers.put("config.providers.vault.param.location", "/usr/vault");
        Properties props = new Properties();
        props.put("sasl.truststore.key", "${vault:/usr/truststore:truststoreKey}");
        props.put("sasl.truststore.password", "${vault:/usr/truststore:truststorePassword}");
        props.put("sasl.truststore.location", "${vault:/usr/truststore:truststoreLocation}");
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props, convertPropertiesToMap(providers));
        assertEquals("/usr/vault", config.originals().get("sasl.truststore.location"));
    }

    @Test
    public void testDocumentationOf() {
        Properties props = new Properties();
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props);

        assertEquals(
                TestIndirectConfigResolution.INDIRECT_CONFIGS_DOC,
                    config.documentationOf(TestIndirectConfigResolution.INDIRECT_CONFIGS)
        );
    }

    @Test
    public void testDocumentationOfExpectNull() {
        Properties props = new Properties();
        TestIndirectConfigResolution config = new TestIndirectConfigResolution(props);

        assertNull(config.documentationOf("xyz"));
    }

    private static class TestIndirectConfigResolution extends AbstractConfig {

        private static final ConfigDef CONFIG;

        public static final String INDIRECT_CONFIGS = "indirect.variables";
        private static final String INDIRECT_CONFIGS_DOC = "Variables whose values can be obtained from ConfigProviders";

        static {
            CONFIG = new ConfigDef().define(INDIRECT_CONFIGS,
                    Type.LIST,
                    "",
                    Importance.LOW,
                    INDIRECT_CONFIGS_DOC);
        }

        public TestIndirectConfigResolution(Map<?, ?> props) {
            super(CONFIG, props, true);
        }

        public TestIndirectConfigResolution(Map<?, ?> props, Map<String, ?> providers) {
            super(CONFIG, props, providers, true);
        }
    }

    private static class ClassTestConfig extends AbstractConfig {
        static final Class<?> DEFAULT_CLASS = FakeMetricsReporter.class;
        static final Class<?> VISIBLE_CLASS = JmxReporter.class;
        static final Class<?> RESTRICTED_CLASS = ConfiguredFakeMetricsReporter.class;

        private static final ConfigDef CONFIG;
        static {
            CONFIG = new ConfigDef().define("class.prop", Type.CLASS, DEFAULT_CLASS, Importance.HIGH, "docs")
                                    .define("list.prop", Type.LIST, Collections.singletonList(DEFAULT_CLASS), Importance.HIGH, "docs");
        }

        public ClassTestConfig() {
            super(CONFIG, new Properties());
        }

        public ClassTestConfig(Object classPropOverride, Object listPropOverride) {
            super(CONFIG, overrideProps(classPropOverride, listPropOverride));
        }

        void checkInstances(Class<?> expectedClassPropClass, Class<?>... expectedListPropClasses) {
            assertEquals(expectedClassPropClass, getConfiguredInstance("class.prop", MetricsReporter.class).getClass());
            List<?> list = getConfiguredInstances("list.prop", MetricsReporter.class);
            for (int i = 0; i < list.size(); i++)
                assertEquals(expectedListPropClasses[i], list.get(i).getClass());
        }

        static void testOverrides() {
            ClassTestConfig testConfig1 = new ClassTestConfig(RESTRICTED_CLASS, Arrays.asList(VISIBLE_CLASS, RESTRICTED_CLASS));
            testConfig1.checkInstances(RESTRICTED_CLASS, VISIBLE_CLASS, RESTRICTED_CLASS);

            ClassTestConfig testConfig2 = new ClassTestConfig(RESTRICTED_CLASS.getName(), Arrays.asList(VISIBLE_CLASS.getName(), RESTRICTED_CLASS.getName()));
            testConfig2.checkInstances(RESTRICTED_CLASS, VISIBLE_CLASS, RESTRICTED_CLASS);

            ClassTestConfig testConfig3 = new ClassTestConfig(RESTRICTED_CLASS.getName(), VISIBLE_CLASS.getName() + "," + RESTRICTED_CLASS.getName());
            testConfig3.checkInstances(RESTRICTED_CLASS, VISIBLE_CLASS, RESTRICTED_CLASS);
        }

        private static Map<String, Object> overrideProps(Object classProp, Object listProp) {
            Map<String, Object> props = new HashMap<>();
            if (classProp != null)
                props.put("class.prop", classProp);
            if (listProp != null)
                props.put("list.prop", listProp);
            return props;
        }
    }

    private static class TestConfig extends AbstractConfig {

        private static final ConfigDef CONFIG;

        public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
        private static final String METRIC_REPORTER_CLASSES_DOC = "A list of classes to use as metrics reporters.";

        static {
            CONFIG = new ConfigDef().define(METRIC_REPORTER_CLASSES_CONFIG,
                                            Type.LIST,
                                            "",
                                            Importance.LOW,
                                            METRIC_REPORTER_CLASSES_DOC);
        }

        public TestConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

    public static class ConfiguredFakeMetricsReporter extends FakeMetricsReporter {
        public static final String EXTRA_CONFIG = "metric.extra_config";
        @Override
        public void configure(Map<String, ?> configs) {
            // Calling get() should have the side effect of marking that config as used.
            // this is required by testUnusedConfigs
            configs.get(EXTRA_CONFIG);
        }
    }
}
