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
package org.apache.kafka.common.security;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

import static org.apache.kafka.common.security.JaasUtils.DISALLOWED_LOGIN_MODULES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests parsing of {@link SaslConfigs#SASL_JAAS_CONFIG} property and verifies that the format
 * and parsing are consistent with JAAS configuration files loaded by the JRE.
 */
public class JaasContextTest {

    private File jaasConfigFile;

    @BeforeEach
    public void setUp() throws IOException {
        jaasConfigFile = TestUtils.tempFile("jaas", ".conf");
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.toString());
        Configuration.setConfiguration(null);
    }

    @AfterEach
    public void tearDown() throws Exception {
        Files.delete(jaasConfigFile.toPath());
        System.clearProperty(DISALLOWED_LOGIN_MODULES_CONFIG);
    }

    @Test
    public void testConfigNoOptions() throws Exception {
        checkConfiguration("test.testConfigNoOptions", LoginModuleControlFlag.REQUIRED, new HashMap<>());
    }

    @Test
    public void testControlFlag() throws Exception {
        LoginModuleControlFlag[] controlFlags = new LoginModuleControlFlag[] {
            LoginModuleControlFlag.REQUIRED,
            LoginModuleControlFlag.REQUISITE,
            LoginModuleControlFlag.SUFFICIENT,
            LoginModuleControlFlag.OPTIONAL
        };
        Map<String, Object> options = new HashMap<>();
        options.put("propName", "propValue");
        for (LoginModuleControlFlag controlFlag : controlFlags) {
            checkConfiguration("test.testControlFlag", controlFlag, options);
        }
    }

    @Test
    public void testSingleOption() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("propName", "propValue");
        checkConfiguration("test.testSingleOption", LoginModuleControlFlag.REQUISITE, options);
    }

    @Test
    public void testMultipleOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        for (int i = 0; i < 10; i++)
            options.put("propName" + i, "propValue" + i);
        checkConfiguration("test.testMultipleOptions", LoginModuleControlFlag.SUFFICIENT, options);
    }

    @Test
    public void testQuotedOptionValue() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("propName", "prop value");
        options.put("propName2", "value1 = 1, value2 = 2");
        String config = String.format("test.testQuotedOptionValue required propName=\"%s\" propName2=\"%s\";", options.get("propName"), options.get("propName2"));
        checkConfiguration(config, "test.testQuotedOptionValue", LoginModuleControlFlag.REQUIRED, options);
    }

    @Test
    public void testQuotedOptionName() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("prop name", "propValue");
        String config = "test.testQuotedOptionName required \"prop name\"=propValue;";
        checkConfiguration(config, "test.testQuotedOptionName", LoginModuleControlFlag.REQUIRED, options);
    }

    @Test
    public void testMultipleLoginModules() throws Exception {
        StringBuilder builder = new StringBuilder();
        int moduleCount = 3;
        Map<Integer, Map<String, Object>> moduleOptions = new HashMap<>();
        for (int i = 0; i < moduleCount; i++) {
            Map<String, Object> options = new HashMap<>();
            options.put("index", "Index" + i);
            options.put("module", "Module" + i);
            moduleOptions.put(i, options);
            String module = jaasConfigProp("test.Module" + i, LoginModuleControlFlag.REQUIRED, options);
            builder.append(' ');
            builder.append(module);
        }
        String jaasConfigProp = builder.toString();

        String clientContextName = "CLIENT";
        Configuration configuration = new JaasConfig(clientContextName, jaasConfigProp);
        AppConfigurationEntry[] dynamicEntries = configuration.getAppConfigurationEntry(clientContextName);
        assertEquals(moduleCount, dynamicEntries.length);

        for (int i = 0; i < moduleCount; i++) {
            AppConfigurationEntry entry = dynamicEntries[i];
            checkEntry(entry, "test.Module" + i, LoginModuleControlFlag.REQUIRED, moduleOptions.get(i));
        }

        String serverContextName = "SERVER";
        writeConfiguration(serverContextName, jaasConfigProp);
        AppConfigurationEntry[] staticEntries = Configuration.getConfiguration().getAppConfigurationEntry(serverContextName);
        for (int i = 0; i < moduleCount; i++) {
            AppConfigurationEntry staticEntry = staticEntries[i];
            checkEntry(staticEntry, dynamicEntries[i].getLoginModuleName(), LoginModuleControlFlag.REQUIRED, dynamicEntries[i].getOptions());
        }
    }

    @Test
    public void testMissingLoginModule() throws Exception {
        checkInvalidConfiguration("  required option1=value1;");
    }

    @Test
    public void testMissingControlFlag() throws Exception {
        checkInvalidConfiguration("test.loginModule option1=value1;");
    }

    @Test
    public void testMissingOptionValue() throws Exception {
        checkInvalidConfiguration("loginModule required option1;");
    }

    @Test
    public void testMissingSemicolon() throws Exception {
        checkInvalidConfiguration("test.testMissingSemicolon required option1=value1");
    }

    @Test
    public void testNumericOptionWithoutQuotes() throws Exception {
        checkInvalidConfiguration("test.testNumericOptionWithoutQuotes required option1=3;");
    }

    @Test
    public void testInvalidControlFlag() throws Exception {
        checkInvalidConfiguration("test.testInvalidControlFlag { option1=3;");
    }

    @Test
    public void testDisallowedLoginModulesSystemProperty() throws Exception {
        //test JndiLoginModule is not allowed by default
        String jaasConfigProp1 = "com.sun.security.auth.module.JndiLoginModule required;";
        assertThrows(IllegalArgumentException.class, () -> configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp1));

        //test ListenerName Override
        writeConfiguration(Arrays.asList(
                "KafkaServer { test.LoginModuleDefault required; };",
                "plaintext.KafkaServer { com.sun.security.auth.module.JndiLoginModule requisite; };"
        ));
        assertThrows(IllegalArgumentException.class, () -> JaasContext.loadServerContext(new ListenerName("plaintext"),
                "SOME-MECHANISM", Collections.emptyMap()));

        //test org.apache.kafka.disallowed.login.modules system property with multiple modules
        System.setProperty(DISALLOWED_LOGIN_MODULES_CONFIG, " com.ibm.security.auth.module.LdapLoginModule , com.ibm.security.auth.module.Krb5LoginModule ");

        String jaasConfigProp2 = "com.ibm.security.auth.module.LdapLoginModule required;";
        assertThrows(IllegalArgumentException.class, () ->  configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp2));

        //test ListenerName Override
        writeConfiguration(Arrays.asList(
                "KafkaServer { test.LoginModuleDefault required; };",
                "plaintext.KafkaServer { com.ibm.security.auth.module.Krb5LoginModule requisite; };"
        ));
        assertThrows(IllegalArgumentException.class, () -> JaasContext.loadServerContext(new ListenerName("plaintext"),
                "SOME-MECHANISM", Collections.emptyMap()));


        //Remove default value for org.apache.kafka.disallowed.login.modules
        System.setProperty(DISALLOWED_LOGIN_MODULES_CONFIG, "");

        checkConfiguration("com.sun.security.auth.module.JndiLoginModule", LoginModuleControlFlag.REQUIRED, new HashMap<>());

        //test ListenerName Override
        writeConfiguration(Arrays.asList(
                "KafkaServer { com.ibm.security.auth.module.LdapLoginModule required; };",
                "plaintext.KafkaServer { com.sun.security.auth.module.JndiLoginModule requisite; };"
        ));
        JaasContext context = JaasContext.loadServerContext(new ListenerName("plaintext"),
                "SOME-MECHANISM", Collections.emptyMap());
        assertEquals(1, context.configurationEntries().size());
        checkEntry(context.configurationEntries().get(0), "com.sun.security.auth.module.JndiLoginModule",
                LoginModuleControlFlag.REQUISITE, Collections.emptyMap());
    }

    @Test
    public void testNumericOptionWithQuotes() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("option1", "3");
        String config = "test.testNumericOptionWithQuotes required option1=\"3\";";
        checkConfiguration(config, "test.testNumericOptionWithQuotes", LoginModuleControlFlag.REQUIRED, options);
    }

    @Test
    public void testLoadForServerWithListenerNameOverride() throws IOException {
        writeConfiguration(Arrays.asList(
            "KafkaServer { test.LoginModuleDefault required; };",
            "plaintext.KafkaServer { test.LoginModuleOverride requisite; };"
        ));
        JaasContext context = JaasContext.loadServerContext(new ListenerName("plaintext"),
            "SOME-MECHANISM", Collections.emptyMap());
        assertEquals("plaintext.KafkaServer", context.name());
        assertEquals(JaasContext.Type.SERVER, context.type());
        assertEquals(1, context.configurationEntries().size());
        checkEntry(context.configurationEntries().get(0), "test.LoginModuleOverride",
            LoginModuleControlFlag.REQUISITE, Collections.emptyMap());
    }

    @Test
    public void testLoadForServerWithListenerNameAndFallback() throws IOException {
        writeConfiguration(Arrays.asList(
            "KafkaServer { test.LoginModule required; };",
            "other.KafkaServer { test.LoginModuleOther requisite; };"
        ));
        JaasContext context = JaasContext.loadServerContext(new ListenerName("plaintext"),
            "SOME-MECHANISM", Collections.emptyMap());
        assertEquals("KafkaServer", context.name());
        assertEquals(JaasContext.Type.SERVER, context.type());
        assertEquals(1, context.configurationEntries().size());
        checkEntry(context.configurationEntries().get(0), "test.LoginModule", LoginModuleControlFlag.REQUIRED,
            Collections.emptyMap());
    }

    @Test
    public void testLoadForServerWithWrongListenerName() throws IOException {
        writeConfiguration("Server", "test.LoginModule required;");
        assertThrows(IllegalArgumentException.class, () -> JaasContext.loadServerContext(new ListenerName("plaintext"),
                "SOME-MECHANISM", Collections.emptyMap()));
    }

    private AppConfigurationEntry configurationEntry(JaasContext.Type contextType, String jaasConfigProp) {
        Password saslJaasConfig = jaasConfigProp == null ? null : new Password(jaasConfigProp);
        JaasContext context = JaasContext.load(contextType, null, contextType.name(), saslJaasConfig);
        List<AppConfigurationEntry> entries = context.configurationEntries();
        assertEquals(1, entries.size());
        return entries.get(0);
    }

    private String controlFlag(LoginModuleControlFlag loginModuleControlFlag) {
        // LoginModuleControlFlag.toString() has format "LoginModuleControlFlag: flag"
        String[] tokens = loginModuleControlFlag.toString().split(" ");
        return tokens[tokens.length - 1];
    }

    private String jaasConfigProp(String loginModule, LoginModuleControlFlag controlFlag, Map<String, Object> options) {
        StringBuilder builder = new StringBuilder();
        builder.append(loginModule);
        builder.append(' ');
        builder.append(controlFlag(controlFlag));
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            builder.append(' ');
            builder.append(entry.getKey());
            builder.append('=');
            builder.append(entry.getValue());
        }
        builder.append(';');
        return builder.toString();
    }

    private void writeConfiguration(String contextName, String jaasConfigProp) throws IOException {
        List<String> lines = Arrays.asList(contextName + " { ", jaasConfigProp, "};");
        writeConfiguration(lines);
    }

    private void writeConfiguration(List<String> lines) throws IOException {
        Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);
        Configuration.setConfiguration(null);
    }

    private void checkConfiguration(String loginModule, LoginModuleControlFlag controlFlag, Map<String, Object> options) throws Exception {
        String jaasConfigProp = jaasConfigProp(loginModule, controlFlag, options);
        checkConfiguration(jaasConfigProp, loginModule, controlFlag, options);
    }

    private void checkEntry(AppConfigurationEntry entry, String loginModule, LoginModuleControlFlag controlFlag, Map<String, ?> options) {
        assertEquals(loginModule, entry.getLoginModuleName());
        assertEquals(controlFlag, entry.getControlFlag());
        assertEquals(options, entry.getOptions());
    }

    private void checkConfiguration(String jaasConfigProp, String loginModule, LoginModuleControlFlag controlFlag, Map<String, Object> options) throws Exception {
        AppConfigurationEntry dynamicEntry = configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp);
        checkEntry(dynamicEntry, loginModule, controlFlag, options);
        assertNull(Configuration.getConfiguration().getAppConfigurationEntry(JaasContext.Type.CLIENT.name()), "Static configuration updated");

        writeConfiguration(JaasContext.Type.SERVER.name(), jaasConfigProp);
        AppConfigurationEntry staticEntry = configurationEntry(JaasContext.Type.SERVER, null);
        checkEntry(staticEntry, loginModule, controlFlag, options);
    }

    private void checkInvalidConfiguration(String jaasConfigProp) throws IOException {
        try {
            writeConfiguration(JaasContext.Type.SERVER.name(), jaasConfigProp);
            AppConfigurationEntry entry = configurationEntry(JaasContext.Type.SERVER, null);
            fail("Invalid JAAS configuration file didn't throw exception, entry=" + entry);
        } catch (SecurityException e) {
            // Expected exception
        }
        try {
            AppConfigurationEntry entry = configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp);
            fail("Invalid JAAS configuration property didn't throw exception, entry=" + entry);
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }
}
