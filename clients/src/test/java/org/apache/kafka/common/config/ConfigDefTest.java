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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

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
        testValidators(Type.INT, Range.between(0, 10), 5, new Object[]{1, 5, 9}, new Object[]{-1, 11});
        testValidators(Type.STRING, ValidString.in("good", "values", "default"), "default",
                new Object[]{"good", "values", "default"}, new Object[]{"bad", "inputs"});
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

    @Test
    public void toMarkdown() {
        final String expected =
            "| Name                                  | Description                                                                                                                                                                                                                                                                                    | Type     | Default                   | Valid Values | Importance |\n" +
            "|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------|--------------|------------|\n" +
            "| ssl.key.password                      | The password of the private key in the key store file. This is optional for client.                                                                                                                                                                                                            | password | null                      |              | high       |\n" +
            "| ssl.keystore.location                 | The location of the key store file. This is optional for client and can be used for two-way authentication for client.                                                                                                                                                                         | string   | null                      |              | high       |\n" +
            "| ssl.enabled.protocols                 | The list of protocols enabled for SSL connections.                                                                                                                                                                                                                                             | list     | [TLSv1.2, TLSv1.1, TLSv1] |              | medium     |\n";

        ConfigDef def = new ConfigDef()
            .define("ssl.key.password", Type.PASSWORD, null, Importance.HIGH, "The password of the private key in the key store file. This is optional for client.")
            .define("ssl.keystore.location", Type.STRING, null, Importance.HIGH, "The location of the key store file. This is optional for client and can be used for two-way authentication for client.")
            .define("ssl.enabled.protocols", Type.STRING, "TLSv1.2,TLSv1.1,TLSv1", Importance.MEDIUM, "The SSL protocol used to generate the SSLContext. Default setting is TLS, which is fine for most cases. Allowed values in recent JVMs are TLS, TLSv1.1 and TLSv1.2. SSL, SSLv2 and SSLv3 may be supported in older JVMs, but their usage is discouraged due to known security vulnerabilities.");
        assertEquals(expected, def.toMarkdown());
    }

}
