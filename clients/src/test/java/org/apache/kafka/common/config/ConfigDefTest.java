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

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConfigDefTest {

    @Test
    public void testBasicTypes() {
        ConfigDef def = new ConfigDef().define("a", Type.INT, 5, Range.between(0, 14), Importance.HIGH, "docs")
                                       .define("b", Type.LONG, Importance.HIGH, "docs")
                                       .define("c", Type.STRING, "hello", Importance.HIGH, "docs")
                                       .define("d", Type.LIST, Importance.HIGH, "docs")
                                       .define("e", Type.DOUBLE, Importance.HIGH, "docs")
                                       .define("f", Type.CLASS, Importance.HIGH, "docs")
                                       .define("g", Type.BOOLEAN, Importance.HIGH, "docs")
                                       .define("h", Type.BOOLEAN, Importance.HIGH, "docs")
                                       .define("i", Type.BOOLEAN, Importance.HIGH, "docs")
                                       .define("j", Type.PASSWORD, Importance.HIGH, "docs");

        Properties props = new Properties();
        props.put("a", "1   ");
        props.put("b", 2);
        props.put("d", " a , b, c");
        props.put("e", 42.5d);
        props.put("f", String.class.getName());
        props.put("g", "true");
        props.put("h", "FalSE");
        props.put("i", "TRUE");
        props.put("j", "password");

        Map<String, Object> vals = def.parse(props);
        assertEquals(1, vals.get("a"));
        assertEquals(2L, vals.get("b"));
        assertEquals("hello", vals.get("c"));
        assertEquals(asList("a", "b", "c"), vals.get("d"));
        assertEquals(42.5d, vals.get("e"));
        assertEquals(String.class, vals.get("f"));
        assertEquals(true, vals.get("g"));
        assertEquals(false, vals.get("h"));
        assertEquals(true, vals.get("i"));
        assertEquals(new Password("password"), vals.get("j"));
        assertEquals(Password.HIDDEN, vals.get("j").toString());
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDefault() {
        new ConfigDef().define("a", Type.INT, "hello", Importance.HIGH, "docs");
    }

    @Test
    public void testNullDefault() {
        ConfigDef def = new ConfigDef().define("a", Type.INT, null, null, null, "docs");
        Map<String, Object> vals = def.parse(new Properties());

        assertEquals(null, vals.get("a"));
    }

    @Test(expected = ConfigException.class)
    public void testMissingRequired() {
        new ConfigDef().define("a", Type.INT, Importance.HIGH, "docs").parse(new HashMap<String, Object>());
    }

    @Test(expected = ConfigException.class)
    public void testDefinedTwice() {
        new ConfigDef().define("a", Type.STRING, Importance.HIGH, "docs").define("a", Type.INT, Importance.HIGH, "docs");
    }

    @Test
    public void testBadInputs() {
        testBadInputs(Type.INT, "hello", "42.5", 42.5, Long.MAX_VALUE, Long.toString(Long.MAX_VALUE), new Object());
        testBadInputs(Type.LONG, "hello", "42.5", Long.toString(Long.MAX_VALUE) + "00", new Object());
        testBadInputs(Type.DOUBLE, "hello", new Object());
        testBadInputs(Type.STRING, new Object());
        testBadInputs(Type.LIST, 53, new Object());
        testBadInputs(Type.BOOLEAN, "hello", "truee", "fals");
    }

    private void testBadInputs(Type type, Object... values) {
        for (Object value : values) {
            Map<String, Object> m = new HashMap<String, Object>();
            m.put("name", value);
            ConfigDef def = new ConfigDef().define("name", type, Importance.HIGH, "docs");
            try {
                def.parse(m);
                fail("Expected a config exception on bad input for value " + value);
            } catch (ConfigException e) {
                // this is good
            }
        }
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDefaultRange() {
        new ConfigDef().define("name", Type.INT, -1, Range.between(0, 10), Importance.HIGH, "docs");
    }

    @Test(expected = ConfigException.class)
    public void testInvalidDefaultString() {
        new ConfigDef().define("name", Type.STRING, "bad", ValidString.in("valid", "values"), Importance.HIGH, "docs");
    }

    @Test
    public void testValidators() {
        testValidators(Type.INT, Range.between(0, 10), 5, new Object[]{1, 5, 9}, new Object[]{-1, 11, null});
        testValidators(Type.STRING, ValidString.in("good", "values", "default"), "default",
                new Object[]{"good", "values", "default"}, new Object[]{"bad", "inputs", null});
        testValidators(Type.LIST, ConfigDef.ValidList.in("1", "2", "3"), "1", new Object[]{"1", "2", "3"}, new Object[]{"4", "5", "6"});
    }

    @Test
    public void testSslPasswords() {
        ConfigDef def = new ConfigDef();
        SslConfigs.addClientSslSupport(def);

        Properties props = new Properties();
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "key_password");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "keystore_password");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "truststore_password");

        Map<String, Object> vals = def.parse(props);
        assertEquals(new Password("key_password"), vals.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        assertEquals(Password.HIDDEN, vals.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG).toString());
        assertEquals(new Password("keystore_password"), vals.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertEquals(Password.HIDDEN, vals.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).toString());
        assertEquals(new Password("truststore_password"), vals.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals(Password.HIDDEN, vals.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString());
    }

    @Test
    public void testNullDefaultWithValidator() {
        final String key = "enum_test";

        ConfigDef def = new ConfigDef();
        def.define(key, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                   ValidString.in("ONE", "TWO", "THREE"), Importance.HIGH, "docs");

        Properties props = new Properties();
        props.put(key, "ONE");
        Map<String, Object> vals = def.parse(props);
        assertEquals("ONE", vals.get(key));
    }

    @Test
    public void testGroupInference() {
        List<String> expected1 = Arrays.asList("group1", "group2");
        ConfigDef def1 = new ConfigDef()
            .define("a", Type.INT, Importance.HIGH, "docs", "group1", 1, Width.SHORT, "a")
            .define("b", Type.INT, Importance.HIGH, "docs", "group2", 1, Width.SHORT, "b")
            .define("c", Type.INT, Importance.HIGH, "docs", "group1", 2, Width.SHORT, "c");

        assertEquals(expected1, def1.groups());

        List<String> expected2 = Arrays.asList("group2", "group1");
        ConfigDef def2 = new ConfigDef()
            .define("a", Type.INT, Importance.HIGH, "docs", "group2", 1, Width.SHORT, "a")
            .define("b", Type.INT, Importance.HIGH, "docs", "group2", 2, Width.SHORT, "b")
            .define("c", Type.INT, Importance.HIGH, "docs", "group1", 2, Width.SHORT, "c");

        assertEquals(expected2, def2.groups());
    }

    @Test
    public void testParseForValidate() {
        Map<String, Object> expectedParsed = new HashMap<>();
        expectedParsed.put("a", 1);
        expectedParsed.put("b", null);
        expectedParsed.put("c", null);
        expectedParsed.put("d", 10);

        Map<String, ConfigValue> expected = new HashMap<>();
        String errorMessageB = "Missing required configuration \"b\" which has no default value.";
        String errorMessageC = "Missing required configuration \"c\" which has no default value.";
        ConfigValue configA = new ConfigValue("a", 1, Collections.<Object>emptyList(), Collections.<String>emptyList());
        ConfigValue configB = new ConfigValue("b", null, Collections.<Object>emptyList(), Arrays.asList(errorMessageB, errorMessageB));
        ConfigValue configC = new ConfigValue("c", null, Collections.<Object>emptyList(), Arrays.asList(errorMessageC));
        ConfigValue configD = new ConfigValue("d", 10, Collections.<Object>emptyList(), Collections.<String>emptyList());
        expected.put("a", configA);
        expected.put("b", configB);
        expected.put("c", configC);
        expected.put("d", configD);

        ConfigDef def = new ConfigDef()
            .define("a", Type.INT, Importance.HIGH, "docs", "group", 1, Width.SHORT, "a", Arrays.asList("b", "c"), new IntegerRecommender(false))
            .define("b", Type.INT, Importance.HIGH, "docs", "group", 2, Width.SHORT, "b", new IntegerRecommender(true))
            .define("c", Type.INT, Importance.HIGH, "docs", "group", 3, Width.SHORT, "c", new IntegerRecommender(true))
            .define("d", Type.INT, Importance.HIGH, "docs", "group", 4, Width.SHORT, "d", Arrays.asList("b"), new IntegerRecommender(false));

        Map<String, String> props = new HashMap<>();
        props.put("a", "1");
        props.put("d", "10");

        Map<String, ConfigValue> configValues = new HashMap<>();

        for (String name: def.configKeys().keySet()) {
            configValues.put(name, new ConfigValue(name));
        }

        Map<String, Object> parsed = def.parseForValidate(props, configValues);

        assertEquals(expectedParsed, parsed);
        assertEquals(expected, configValues);
    }

    @Test
    public void testValidate() {
        Map<String, ConfigValue> expected = new HashMap<>();
        String errorMessageB = "Missing required configuration \"b\" which has no default value.";
        String errorMessageC = "Missing required configuration \"c\" which has no default value.";

        ConfigValue configA = new ConfigValue("a", 1, Arrays.<Object>asList(1, 2, 3), Collections.<String>emptyList());
        ConfigValue configB = new ConfigValue("b", null, Arrays.<Object>asList(4, 5), Arrays.asList(errorMessageB, errorMessageB));
        ConfigValue configC = new ConfigValue("c", null, Arrays.<Object>asList(4, 5), Arrays.asList(errorMessageC));
        ConfigValue configD = new ConfigValue("d", 10, Arrays.<Object>asList(1, 2, 3), Collections.<String>emptyList());

        expected.put("a", configA);
        expected.put("b", configB);
        expected.put("c", configC);
        expected.put("d", configD);

        ConfigDef def = new ConfigDef()
            .define("a", Type.INT, Importance.HIGH, "docs", "group", 1, Width.SHORT, "a", Arrays.asList("b", "c"), new IntegerRecommender(false))
            .define("b", Type.INT, Importance.HIGH, "docs", "group", 2, Width.SHORT, "b", new IntegerRecommender(true))
            .define("c", Type.INT, Importance.HIGH, "docs", "group", 3, Width.SHORT, "c", new IntegerRecommender(true))
            .define("d", Type.INT, Importance.HIGH, "docs", "group", 4, Width.SHORT, "d", Arrays.asList("b"), new IntegerRecommender(false));

        Map<String, String> props = new HashMap<>();
        props.put("a", "1");
        props.put("d", "10");

        List<ConfigValue> configs = def.validate(props);
        for (ConfigValue config : configs) {
            String name = config.name();
            ConfigValue expectedConfig = expected.get(name);
            assertEquals(expectedConfig, config);
        }
    }

    @Test
    public void testValidateMissingConfigKey() {
        Map<String, ConfigValue> expected = new HashMap<>();
        String errorMessageB = "Missing required configuration \"b\" which has no default value.";
        String errorMessageC = "Missing required configuration \"c\" which has no default value.";
        String errorMessageD = "d is referred in the dependents, but not defined.";

        ConfigValue configA = new ConfigValue("a", 1, Arrays.<Object>asList(1, 2, 3), Collections.<String>emptyList());
        ConfigValue configB = new ConfigValue("b", null, Arrays.<Object>asList(4, 5), Arrays.asList(errorMessageB));
        ConfigValue configC = new ConfigValue("c", null, Arrays.<Object>asList(4, 5), Arrays.asList(errorMessageC));
        ConfigValue configD = new ConfigValue("d", null, Collections.emptyList(), Arrays.asList(errorMessageD));
        configD.visible(false);

        expected.put("a", configA);
        expected.put("b", configB);
        expected.put("c", configC);
        expected.put("d", configD);

        ConfigDef def = new ConfigDef()
            .define("a", Type.INT, Importance.HIGH, "docs", "group", 1, Width.SHORT, "a", Arrays.asList("b", "c", "d"), new IntegerRecommender(false))
            .define("b", Type.INT, Importance.HIGH, "docs", "group", 2, Width.SHORT, "b", new IntegerRecommender(true))
            .define("c", Type.INT, Importance.HIGH, "docs", "group", 3, Width.SHORT, "c", new IntegerRecommender(true));

        Map<String, String> props = new HashMap<>();
        props.put("a", "1");

        List<ConfigValue> configs = def.validate(props);
        for (ConfigValue config: configs) {
            String name = config.name();
            ConfigValue expectedConfig = expected.get(name);
            assertEquals(expectedConfig, config);
        }
    }

    @Test
    public void testValidateCannotParse() {
        Map<String, ConfigValue> expected = new HashMap<>();
        String errorMessageB = "Invalid value non_integer for configuration a: Not a number of type INT";
        ConfigValue configA = new ConfigValue("a", null, Collections.emptyList(), Arrays.asList(errorMessageB));
        expected.put("a", configA);

        ConfigDef def = new ConfigDef().define("a", Type.INT, Importance.HIGH, "docs");
        Map<String, String> props = new HashMap<>();
        props.put("a", "non_integer");

        List<ConfigValue> configs = def.validate(props);
        for (ConfigValue config: configs) {
            String name = config.name();
            ConfigValue expectedConfig = expected.get(name);
            assertEquals(expectedConfig, config);
        }
    }

    private static class IntegerRecommender implements ConfigDef.Recommender {

        private boolean hasParent;

        public IntegerRecommender(boolean hasParent) {
            this.hasParent = hasParent;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            List<Object> values = new LinkedList<>();
            if (!hasParent) {
                values.addAll(Arrays.asList(1, 2, 3));
            } else {
                values.addAll(Arrays.asList(4, 5));
            }
            return values;
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private void testValidators(Type type, Validator validator, Object defaultVal, Object[] okValues, Object[] badValues) {
        ConfigDef def = new ConfigDef().define("name", type, defaultVal, validator, Importance.HIGH, "docs");

        for (Object value : okValues) {
            Map<String, Object> m = new HashMap<String, Object>();
            m.put("name", value);
            def.parse(m);
        }

        for (Object value : badValues) {
            Map<String, Object> m = new HashMap<String, Object>();
            m.put("name", value);
            try {
                def.parse(m);
                fail("Expected a config exception due to invalid value " + value);
            } catch (ConfigException e) {
                // this is good
            }
        }
    }
}
