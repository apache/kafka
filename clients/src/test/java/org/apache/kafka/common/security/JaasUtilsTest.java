/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.LoginType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests parsing of {@link SaslConfigs#SASL_JAAS_CONFIG} property and verifies that the format
 * and parsing are consistent with JAAS configuration files loaded by the JRE.
 */
public class JaasUtilsTest {

    private File jaasConfigFile;

    @Before
    public void setUp() throws IOException {
        jaasConfigFile = File.createTempFile("jaas", ".conf");
        jaasConfigFile.deleteOnExit();
        Configuration.setConfiguration(null);
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.toString());
    }

    @After
    public void tearDown() {
        jaasConfigFile.delete();
    }

    @Test
    public void testConfigNoOptions() throws Exception {
        checkConfiguration("test.testConfigNoOptions", LoginModuleControlFlag.REQUIRED, new HashMap<String, Object>());
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

        Map<String, Object> configs = new HashMap<>();
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigProp));
        Configuration configuration = JaasUtils.jaasConfig(LoginType.CLIENT, configs);
        AppConfigurationEntry[] dynamicEntries = configuration.getAppConfigurationEntry(LoginType.CLIENT.contextName());
        assertEquals(moduleCount, dynamicEntries.length);

        for (int i = 0; i < moduleCount; i++) {
            AppConfigurationEntry entry = dynamicEntries[i];
            checkEntry(entry, "test.Module" + i, LoginModuleControlFlag.REQUIRED, moduleOptions.get(i));
        }

        writeConfiguration(LoginType.SERVER, jaasConfigProp);
        AppConfigurationEntry[] staticEntries = Configuration.getConfiguration().getAppConfigurationEntry(LoginType.SERVER.contextName());
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
    public void testNumericOptionWithQuotes() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("option1", "3");
        String config = "test.testNumericOptionWithQuotes required option1=\"3\";";
        checkConfiguration(config, "test.testNumericOptionWithQuotes", LoginModuleControlFlag.REQUIRED, options);
    }

    private AppConfigurationEntry configurationEntry(LoginType loginType, String jaasConfigProp) {
        Map<String, Object> configs = new HashMap<>();
        if (jaasConfigProp != null)
            configs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigProp));
        Configuration configuration = JaasUtils.jaasConfig(loginType, configs);
        AppConfigurationEntry[] entry = configuration.getAppConfigurationEntry(loginType.contextName());
        assertEquals(1, entry.length);
        return entry[0];
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

    private void writeConfiguration(LoginType loginType, String jaasConfigProp) throws IOException {
        List<String> lines = Arrays.asList(loginType.contextName() + " { ", jaasConfigProp, "};");
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
        AppConfigurationEntry dynamicEntry = configurationEntry(LoginType.CLIENT, jaasConfigProp);
        checkEntry(dynamicEntry, loginModule, controlFlag, options);
        assertNull("Static configuration updated", Configuration.getConfiguration().getAppConfigurationEntry(LoginType.CLIENT.contextName()));

        writeConfiguration(LoginType.SERVER, jaasConfigProp);
        AppConfigurationEntry staticEntry = configurationEntry(LoginType.SERVER, null);
        checkEntry(staticEntry, loginModule, controlFlag, options);
    }

    private void checkInvalidConfiguration(String jaasConfigProp) throws IOException {
        try {
            writeConfiguration(LoginType.SERVER, jaasConfigProp);
            AppConfigurationEntry entry = configurationEntry(LoginType.SERVER, null);
            fail("Invalid JAAS configuration file didn't throw exception, entry=" + entry);
        } catch (SecurityException e) {
            // Expected exception
        }
        try {
            AppConfigurationEntry entry = configurationEntry(LoginType.CLIENT, jaasConfigProp);
            fail("Invalid JAAS configuration property didn't throw exception, entry=" + entry);
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }
}
