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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenMock;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils.getConfiguredInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigurationUtilsTest extends OAuthBearerTest {

    private static final String URL_CONFIG_NAME = "fictitious.url.config";
    private static final String FILE_CONFIG_NAME = "fictitious.file.config";

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testUrl() {
        testUrl("http://www.example.com");
    }

    @Test
    public void testUrlWithSuperfluousWhitespace() {
        testUrl(String.format("  %s  ", "http://www.example.com"));
    }

    @Test
    public void testUrlCaseInsensitivity() {
        testUrl("HTTPS://WWW.EXAMPLE.COM");
    }

    @Test
    public void testUrlFile() {
        assertThrowsWithMessage(ConfigException.class, () -> testFileUrl("file:///tmp/foo.txt"), "that doesn't exist");
    }

    @Test
    public void testUrlFullPath() {
        testUrl("https://myidp.example.com/oauth2/default/v1/token");
    }

    @Test
    public void testUrlMissingProtocol() {
        assertThrowsWithMessage(ConfigException.class, () -> testUrl("www.example.com"), "no protocol");
    }

    @Test
    public void testUrlInvalidProtocol() {
        assertThrowsWithMessage(ConfigException.class, () -> testFileUrl("ftp://ftp.example.com"), "invalid protocol");
    }

    @Test
    public void testUrlNull() {
        assertThrowsWithMessage(ConfigException.class, () -> testUrl(null), "is required");
    }

    @Test
    public void testUrlEmptyString() {
        assertThrowsWithMessage(ConfigException.class, () -> testUrl(""), "is required");
    }

    @Test
    public void testUrlWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> testUrl("    "), "is required");
    }

    @Test
    public void testFile() throws IOException {
        File file = TestUtils.tempFile("some contents!");
        testFile(file.getAbsolutePath());
    }

    @Test
    public void testFileWithSuperfluousWhitespace() throws IOException {
        File file = TestUtils.tempFile();
        testFile(String.format("  %s  ", file.getAbsolutePath()));
    }

    @Test
    public void testFileDoesNotExist() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile(new File("/tmp/not/a/real/file.txt").toURI().toURL().toString()), "that doesn't exist");
    }

    @Test
    public void testFileUnreadable() throws IOException {
        File file = TestUtils.tempFile();

        if (!file.setReadable(false))
            throw new IllegalStateException(String.format("Can't test file permissions as test couldn't programmatically make temp file %s un-readable", file.getAbsolutePath()));

        assertThrowsWithMessage(ConfigException.class, () -> testFile(file.getAbsolutePath()), "that doesn't have read permission");
    }

    @Test
    public void testFileNull() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile(null), "is required");
    }

    @Test
    public void testFileEmptyString() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile(""), "is required");
    }

    @Test
    public void testFileWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile("    "), "is required");
    }

    @Test
    public void testThrowIfURLIsNotAllowed() {
        String url = "http://www.example.com";
        String fileUrl = "file:///etc/passwd";
        ConfigurationUtils cu = new ConfigurationUtils(Map.of());

        // By default, no URL is allowed
        assertThrowsWithMessage(ConfigException.class, () -> cu.throwIfURLIsNotAllowed(URL_CONFIG_NAME, url),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        assertThrowsWithMessage(ConfigException.class, () -> cu.throwIfURLIsNotAllowed(FILE_CONFIG_NAME, fileUrl),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);

        // add one url into allowed list
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, url);
        assertDoesNotThrow(() -> cu.throwIfURLIsNotAllowed(URL_CONFIG_NAME, url));
        assertThrowsWithMessage(ConfigException.class, () -> cu.throwIfURLIsNotAllowed(FILE_CONFIG_NAME, fileUrl),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);

        // add all urls into allowed list
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, url + "," + fileUrl);
        assertDoesNotThrow(() -> cu.throwIfURLIsNotAllowed(URL_CONFIG_NAME, url));
        assertDoesNotThrow(() -> cu.throwIfURLIsNotAllowed(FILE_CONFIG_NAME, fileUrl));
    }

    @Test
    public void testThrowIfFileIsNotAllowed() {
        String file1 = "file1";
        String file2 = "file2";
        ConfigurationUtils cu = new ConfigurationUtils(Map.of());

        // By default, no file is allowed
        assertThrowsWithMessage(ConfigException.class, () -> cu.throwIfFileIsNotAllowed(FILE_CONFIG_NAME, file1),
            ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
        assertThrowsWithMessage(ConfigException.class, () -> cu.throwIfFileIsNotAllowed(FILE_CONFIG_NAME, file1),
            ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);

        // add one file into allowed list
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, file1);
        assertDoesNotThrow(() -> cu.throwIfFileIsNotAllowed(FILE_CONFIG_NAME, file1));
        assertThrowsWithMessage(ConfigException.class, () -> cu.throwIfFileIsNotAllowed(FILE_CONFIG_NAME, file2),
            ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);

        // add all files into allowed list
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, file1 + "," + file2);
        assertDoesNotThrow(() -> cu.throwIfFileIsNotAllowed(FILE_CONFIG_NAME, file1));
        assertDoesNotThrow(() -> cu.throwIfFileIsNotAllowed(FILE_CONFIG_NAME, file2));
    }

    @Test
    public void testConstructorSetsPrefixToSaslMechanism() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of(), OAUTHBEARER_MECHANISM);
        assertEquals("oauthbearer.", cu.prefix());
    }

    @ParameterizedTest
    @NullSource
    @EmptySource
    public void testConstructorSetsPrefixToNull(String saslMechanism) {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of(), saslMechanism);
        assertNull(cu.prefix());
    }

    @Test
    public void testContainsKeyReturnsTrueWhenKeyIsPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "value"));
        assertTrue(cu.containsKey("key"));
    }

    @Test
    public void testContainsKeyReturnsFalseWhenKeyIsNotPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "value"));
        assertFalse(cu.containsKey("key1"));
    }

    @Test
    public void testValidateIntegerReturnsValueWhenPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", 42));
        assertEquals(42, cu.validateInteger("key", true));
    }

    @Test
    public void testValidateIntegerThrowsWhenRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertThrows(ConfigException.class,
                () -> cu.validateInteger("key", true));
    }

    @Test
    public void testValidateIntegerReturnsNullWhenNotRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertNull(cu.validateInteger("key", false));
    }

    @Test
    public void testValidateLongThrowsWhenMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertThrows(ConfigException.class, () -> cu.validateLong("key"));
    }

    @Test
    public void testValidateLongReturnsValueWhenPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", 42L));
        assertEquals(42L, cu.validateLong("key", true));
    }

    @Test
    public void testValidateLongThrowsWhenRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertThrows(ConfigException.class, () -> cu.validateLong("key", true));
    }

    @Test
    public void testValidateLongReturnsNullWhenNotRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertNull(cu.validateLong("key", false));
    }

    @Test
    public void testValidateLongThrowsWhenValueBelowMin() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", 5L));
        assertThrows(ConfigException.class, () -> cu.validateLong("key", true, 10L));
    }

    @Test
    public void testValidateLongReturnsValueWhenValueEqualsMin() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", 10L));
        assertEquals(10L, cu.validateLong("key", true, 10L));
    }

    @Test
    public void testValidateLongReturnsValueWhenValueAboveMin() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", 15L));
        assertEquals(15L, cu.validateLong("key", true, 10L));
    }

    @Test
    public void testValidateLongReturnsValueWhenMinIsNull() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", 42L));
        assertEquals(42L, cu.validateLong("key", true, null));
    }

    @Test
    public void testValidatePasswordReturnsValueWhenPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", new Password("secret")));
        assertEquals("secret", cu.validatePassword("key"));
    }

    @Test
    public void testValidatePasswordThrowsWhenMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertThrows(ConfigException.class, () -> cu.validatePassword("key"));
    }

    @Test
    public void testValidatePasswordThrowsWhenBlank() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", new Password("   ")));
        assertThrows(ConfigException.class, () -> cu.validatePassword("key"));
    }

    @Test
    public void testValidatePasswordTrimsWhitespace() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", new Password("  secret  ")));
        assertEquals("secret", cu.validatePassword("key"));
    }

    @Test
    public void testValidateStringReturnsValueWhenPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "value"));
        assertEquals("value", cu.validateString("key", true));
    }

    @Test
    public void testValidateStringTrimsWhitespace() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "  value  "));
        assertEquals("value", cu.validateString("key", true));
    }

    @Test
    public void testValidateStringThrowsWhenRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertThrows(ConfigException.class, () -> cu.validateString("key", true));
    }

    @Test
    public void testValidateStringThrowsWhenRequiredAndBlank() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "   "));
        assertThrows(ConfigException.class, () -> cu.validateString("key", true));
    }

    @Test
    public void testValidateStringReturnsNullWhenNotRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertNull(cu.validateString("key", false));
    }

    @Test
    public void testValidateStringReturnsNullWhenNotRequiredAndBlank() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "   "));
        assertNull(cu.validateString("key", false));
    }

    @Test
    public void testValidateBooleanReturnsTrueWhenPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", true));
        assertEquals(true, cu.validateBoolean("key", true));
    }

    @Test
    public void testValidateBooleanReturnsFalseWhenPresent() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", false));
        assertEquals(false, cu.validateBoolean("key", true));
    }

    @Test
    public void testValidateBooleanThrowsWhenRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertThrows(ConfigException.class, () -> cu.validateBoolean("key", true));
    }

    @Test
    public void testValidateBooleanReturnsNullWhenNotRequiredAndMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap());
        assertNull(cu.validateBoolean("key", false));
    }

    @Test
    public void testGetReturnsValueByName() {
        ConfigurationUtils cu = new ConfigurationUtils(Map.of("key", "value"), null);
        assertEquals("value", cu.get("key"));
    }

    @Test
    public void testGetReturnsNullWhenMissing() {
        ConfigurationUtils cu = new ConfigurationUtils(Collections.emptyMap(), null);
        assertNull(cu.get("missing"));
    }

    @Test
    public void testGetReturnsPrefixedValueOverUnprefixed() {
        String prefix = ListenerName.saslMechanismPrefix(OAUTHBEARER_MECHANISM);
        ConfigurationUtils cu = new ConfigurationUtils(
                Map.of(
                        prefix + "key", "prefixed-value",
                        "key", "unprefixed-value"
                ),
                OAUTHBEARER_MECHANISM);
        assertEquals("prefixed-value", cu.get("key"));
    }

    @Test
    public void testGetReturnsUnprefixedValueWhenPrefixedNotFound() {
        ConfigurationUtils cu = new ConfigurationUtils(
                Map.of("key", "unprefixed-value"),
                OAUTHBEARER_MECHANISM);
        assertEquals("unprefixed-value", cu.get("key"));
    }

    @Test
    public void testGetReturnsNullWhenNeitherPrefixedNorUnprefixedFound() {
        ConfigurationUtils cu = new ConfigurationUtils(
                Collections.emptyMap(),
                OAUTHBEARER_MECHANISM);
        assertNull(cu.get("key"));
    }

    @Test
    public void testGetIgnoresPrefixWhenSaslMechanismIsNull() {
        ConfigurationUtils cu = new ConfigurationUtils(
                Map.of("key", "value"),
                null);
        assertEquals("value", cu.get("key"));
    }

    @Test
    public void testGetIgnoresPrefixWhenSaslMechanismIsBlank() {
        ConfigurationUtils cu = new ConfigurationUtils(
                Map.of("key", "value"),
                "   ");
        assertEquals("value", cu.get("key"));
    }

    @Test
    public void testGetConfiguredInstanceFromClassName() {
        Map<String, ?> configs = Map.of("config.key", OAuthBearerTokenMock.class.getName());
        OAuthBearerToken result = getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", OAuthBearerToken.class);
        assertNotNull(result);
        assertInstanceOf(OAuthBearerToken.class, result);
    }

    @Test
    public void testGetConfiguredInstanceFromClass() {
        Map<String, ?> configs = Map.of("config.key", OAuthBearerTokenMock.class);
        OAuthBearerToken result = getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", OAuthBearerToken.class);
        assertNotNull(result);
        assertInstanceOf(OAuthBearerToken.class, result);
    }

    @Test
    public void testGetConfiguredInstanceThrowsWhenConfigIsNull() {
        Map<String, ?> configs = Collections.emptyMap();
        assertThrows(ConfigException.class, () -> getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", OAuthBearerToken.class));
    }

    @Test
    public void testGetConfiguredInstanceThrowsWhenConfigIsWrongType() {
        Map<String, ?> configs = Map.of("config.key", 42);
        assertThrows(ConfigException.class, () -> getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", OAuthBearerToken.class));
    }

    @Test
    public void testGetConfiguredInstanceThrowsWhenClassNameIsInvalid() {
        Map<String, ?> configs = Map.of("config.key", "com.nonexistent.ClassName");
        assertThrows(ConfigException.class, () -> getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", OAuthBearerToken.class));
    }

    @Test
    public void testGetConfiguredInstanceThrowsWhenClassIsWrongType() {
        Map<String, ?> configs = Map.of("config.key", String.class);
        assertThrows(ConfigException.class, () -> getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", OAuthBearerToken.class));
    }

    @Test
    public void testGetConfiguredInstanceCallsConfigureOnOAuthBearerConfigurable() {
        Map<String, ?> configs = Map.of("config.key", MyConfigurableImpl.class);
        MyConfigurableImpl result = getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", MyConfigurableImpl.class);
        assertTrue(result.configureCalled);
    }

    @Test
    public void testGetConfiguredInstanceThrowsWhenConfigureThrows() {
        Map<String, ?> configs = Map.of("config.key", MyFailingConfigurableImpl.class);
        assertThrows(ConfigException.class, () -> getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", MyFailingConfigurableImpl.class));
    }

    @Test
    public void testGetConfiguredInstanceThrowsWhenClassCannotBeInstantiated() {
        Map<String, ?> configs = Map.of("config.key", NoDefaultConstructorImpl.class);
        assertThrows(ConfigException.class, () -> getConfiguredInstance(configs, OAUTHBEARER_MECHANISM,
                getJaasConfigEntries(), "config.key", NoDefaultConstructorImpl.class));
    }

    private void testUrl(String value) {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, value == null ? "" : value);
        Map<String, Object> configs = Collections.singletonMap(URL_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateUrl(URL_CONFIG_NAME);
    }

    private void testFile(String value) {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, value == null ? "" : value);
        Map<String, Object> configs = Collections.singletonMap(FILE_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateFile(FILE_CONFIG_NAME);
    }

    private void testFileUrl(String value) {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, value == null ? "" : value);
        Map<String, Object> configs = Collections.singletonMap(URL_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateFileUrl(URL_CONFIG_NAME);
    }

    public static class MyConfigurableImpl implements OAuthBearerConfigurable {
        boolean configureCalled = false;

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism,
                              List<AppConfigurationEntry> jaasConfigEntries) {
            configureCalled = true;
        }
    }

    public static class MyFailingConfigurableImpl implements OAuthBearerConfigurable {
        @Override
        public void configure(Map<String, ?> configs, String saslMechanism,
                              List<AppConfigurationEntry> jaasConfigEntries) {
            throw new RuntimeException("configure() failed");
        }
    }

    public static class NoDefaultConstructorImpl implements OAuthBearerConfigurable {
        public NoDefaultConstructorImpl(String arg) { }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism,
                              List<AppConfigurationEntry> jaasConfigEntries) { }
    }
}
