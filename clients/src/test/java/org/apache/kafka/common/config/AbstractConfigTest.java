/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.FakeMetricsReporter;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
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
    public void testUnused() {
        Properties props = new Properties();
        String configValue = "org.apache.kafka.common.config.AbstractConfigTest$ConfiguredFakeMetricsReporter";
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        props.put(FakeMetricsReporterConfig.EXTRA_CONFIG, "my_value");
        TestConfig config = new TestConfig(props);

        assertTrue("metric.extra_config should be marked unused before getConfiguredInstances is called",
            config.unused().contains(FakeMetricsReporterConfig.EXTRA_CONFIG));

        config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        assertTrue("All defined configurations should be marked as used", config.unused().isEmpty());
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
                    return null;
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
        testConfig = new ClassTestConfig(ClassTestConfig.RESTRICTED_CLASS, Arrays.asList(ClassTestConfig.RESTRICTED_CLASS));
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

    private static class ClassTestConfig extends AbstractConfig {
        static final Class<?> DEFAULT_CLASS = FakeMetricsReporter.class;
        static final Class<?> VISIBLE_CLASS = JmxReporter.class;
        static final Class<?> RESTRICTED_CLASS = ConfiguredFakeMetricsReporter.class;

        private static final ConfigDef CONFIG;
        static {
            CONFIG = new ConfigDef().define("class.prop", Type.CLASS, DEFAULT_CLASS, Importance.HIGH, "docs")
                                    .define("list.prop", Type.LIST, Arrays.asList(DEFAULT_CLASS), Importance.HIGH, "docs");
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
        @Override
        public void configure(Map<String, ?> configs) {
            FakeMetricsReporterConfig config = new FakeMetricsReporterConfig(configs);

            // Calling getString() should have the side effect of marking that config as used.
            config.getString(FakeMetricsReporterConfig.EXTRA_CONFIG);
        }
    }

    public static class FakeMetricsReporterConfig extends AbstractConfig {

        public static final String EXTRA_CONFIG = "metric.extra_config";
        private static final String EXTRA_CONFIG_DOC = "An extraneous configuration string.";
        private static final ConfigDef CONFIG = new ConfigDef().define(
                EXTRA_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, EXTRA_CONFIG_DOC);


        public FakeMetricsReporterConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }
}
