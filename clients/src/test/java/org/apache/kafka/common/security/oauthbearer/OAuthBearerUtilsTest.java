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
package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.maybeCreateSslResource;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.throwIfURLIsNotAllowed;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateClaimExpiration;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateClaimIssuedAt;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateClaimNameOverride;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateClaimScopes;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateClaimSubject;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateFileUrl;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateUrl;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OAuthBearerUtilsTest extends OAuthBearerTest {

    private static final String URL_CONFIG_NAME = "url";

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
        testUrl("file:///tmp/foo.txt");
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
        assertThrowsWithMessage(ConfigException.class, () -> testUrl("ftp://ftp.example.com"), "invalid protocol");
    }

    @Test
    public void testUrlNull() {
        assertThrows(ConfigException.class, () -> testUrl(null));
    }

    @Test
    public void testUrlEmptyString() {
        assertThrows(ConfigException.class, () -> testUrl(""));
    }

    @Test
    public void testUrlWhitespace() {
        assertThrows(ConfigException.class, () -> testUrl("    "));
    }

    @Test
    public void testFile() throws IOException {
        File file = TestUtils.tempFile("some contents!");
        testFile(file.toURI().toURL().toString());
    }

    @Test
    public void testFileWithSuperfluousWhitespace() throws IOException {
        File file = TestUtils.tempFile();
        testFile(String.format("  %s  ", file.toURI().toURL()));
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

        assertThrowsWithMessage(ConfigException.class, () -> testFile(file.toURI().toURL().toString()), "that doesn't have read permission");
    }

    @Test
    public void testFileNull() {
        assertThrows(ConfigException.class, () -> testFile(null));
    }

    @Test
    public void testFileEmptyString() {
        assertThrows(ConfigException.class, () -> testFile(""));
    }

    @Test
    public void testFileWhitespace() {
        assertThrows(ConfigException.class, () -> testFile("    "));
    }

    @Test
    public void testThrowIfURLIsNotAllowed() {
        String url = "http://www.example.com";
        String fileUrl = "file:///etc/passwd";

        // By default, no URL is allowed
        assertThrowsWithMessage(ConfigException.class, () -> throwIfURLIsNotAllowed(url),
            ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        assertThrowsWithMessage(ConfigException.class, () -> throwIfURLIsNotAllowed(fileUrl),
            ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);

        // add one url into allowed list
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, url);
        assertDoesNotThrow(() -> throwIfURLIsNotAllowed(url));
        assertThrowsWithMessage(ConfigException.class, () -> throwIfURLIsNotAllowed(fileUrl),
            ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);

        // add all urls into allowed list
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, url + "," + fileUrl);
        assertDoesNotThrow(() -> throwIfURLIsNotAllowed(url));
        assertDoesNotThrow(() -> throwIfURLIsNotAllowed(fileUrl));
    }

    @Test
    public void testValidateScopes() {
        Set<String> scopes = validateClaimScopes("scope", Arrays.asList("  a  ", "    b    "));

        assertEquals(2, scopes.size());
        assertTrue(scopes.contains("a"));
        assertTrue(scopes.contains("b"));
    }

    @Test
    public void testValidateScopesDisallowsDuplicates() {
        assertThrows(JwtValidatorException.class, () -> validateClaimScopes("scope", Arrays.asList("a", "b", "a")));
        assertThrows(JwtValidatorException.class, () -> validateClaimScopes("scope", Arrays.asList("a", "b", "  a  ")));
    }

    @Test
    public void testValidateScopesDisallowsEmptyNullAndWhitespace() {
        assertThrows(JwtValidatorException.class, () -> validateClaimScopes("scope", Arrays.asList("a", "")));
        assertThrows(JwtValidatorException.class, () -> validateClaimScopes("scope", Arrays.asList("a", null)));
        assertThrows(JwtValidatorException.class, () -> validateClaimScopes("scope", Arrays.asList("a", "  ")));
    }

    @Test
    public void testValidateScopesResultIsImmutable() {
        SortedSet<String> callerSet = new TreeSet<>(Arrays.asList("a", "b", "c"));
        Set<String> scopes = validateClaimScopes("scope", callerSet);

        assertEquals(3, scopes.size());

        callerSet.add("d");
        assertEquals(4, callerSet.size());
        assertTrue(callerSet.contains("d"));
        assertEquals(3, scopes.size());
        assertFalse(scopes.contains("d"));

        callerSet.remove("c");
        assertEquals(3, callerSet.size());
        assertFalse(callerSet.contains("c"));
        assertEquals(3, scopes.size());
        assertTrue(scopes.contains("c"));

        callerSet.clear();
        assertEquals(3, scopes.size());
    }

    @Test
    public void testValidateScopesResultThrowsExceptionOnMutation() {
        SortedSet<String> callerSet = new TreeSet<>(Arrays.asList("a", "b", "c"));
        Set<String> scopes = validateClaimScopes("scope", callerSet);
        assertThrows(UnsupportedOperationException.class, scopes::clear);
    }

    @Test
    public void testValidateExpiration() {
        Long expected = 1L;
        Long actual = validateClaimExpiration("exp", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateExpirationAllowsZero() {
        Long expected = 0L;
        Long actual = validateClaimExpiration("exp", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateExpirationDisallowsNull() {
        assertThrows(JwtValidatorException.class, () -> validateClaimExpiration("exp", null));
    }

    @Test
    public void testValidateExpirationDisallowsNegatives() {
        assertThrows(JwtValidatorException.class, () -> validateClaimExpiration("exp", -1L));
    }

    @Test
    public void testValidateSubject() {
        String expected = "jdoe";
        String actual = validateClaimSubject("sub", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateSubjectDisallowsEmptyNullAndWhitespace() {
        assertThrows(JwtValidatorException.class, () -> validateClaimSubject("sub", ""));
        assertThrows(JwtValidatorException.class, () -> validateClaimSubject("sub", null));
        assertThrows(JwtValidatorException.class, () -> validateClaimSubject("sub", "  "));
    }

    @Test
    public void testValidateClaimNameOverride() {
        String expected = "email";
        String actual = validateClaimNameOverride("sub", String.format("  %s  ", expected));
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateClaimNameOverrideDisallowsEmptyNullAndWhitespace() {
        assertThrows(JwtValidatorException.class, () -> validateClaimSubject("sub", ""));
        assertThrows(JwtValidatorException.class, () -> validateClaimSubject("sub", null));
        assertThrows(JwtValidatorException.class, () -> validateClaimSubject("sub", "  "));
    }

    @Test
    public void testValidateIssuedAt() {
        Long expected = 1L;
        Long actual = validateClaimIssuedAt("iat", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateIssuedAtAllowsZero() {
        Long expected = 0L;
        Long actual = validateClaimIssuedAt("iat", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateIssuedAtAllowsNull() {
        Long expected = null;
        Long actual = validateClaimIssuedAt("iat", expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testValidateIssuedAtDisallowsNegatives() {
        assertThrows(JwtValidatorException.class, () -> validateClaimIssuedAt("iat", -1L));
    }

    @Test
    public void testSSLClientConfig() {
        String sslKeystore = "test.keystore.jks";
        String sslTruststore = "test.truststore.jks";

        Map<String, Object> options = new HashMap<>();
        options.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystore);
        options.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "$3cr3+");
        options.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststore);

        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(options);
        Map<String, ?> sslClientConfig = OAuthBearerUtils.getSslClientConfig(jaasConfig);
        assertNotNull(sslClientConfig);
        assertEquals(sslKeystore, sslClientConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals(sslTruststore, sslClientConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, sslClientConfig.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    @Test
    public void testShouldUseSslClientConfig() throws Exception {
        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(Collections.emptyMap());
        assertFalse(maybeCreateSslResource(new URL("http://www.example.com"), jaasConfig).isPresent());
        assertTrue(maybeCreateSslResource(new URL("https://www.example.com"), jaasConfig).isPresent());
        assertFalse(maybeCreateSslResource(new URL("file:///tmp/test.txt"), jaasConfig).isPresent());
    }

    private void testUrl(String value) {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, value == null ? "" : value);
        Map<String, Object> configs = Collections.singletonMap(URL_CONFIG_NAME, value);
        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, OAUTHBEARER_MECHANISM);
        validateUrl(oauthConfig, URL_CONFIG_NAME);
    }

    private void testFile(String value) {
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, value == null ? "" : value);
        Map<String, Object> configs = Collections.singletonMap(URL_CONFIG_NAME, value);
        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, OAUTHBEARER_MECHANISM);
        validateFileUrl(oauthConfig, URL_CONFIG_NAME);
    }
}
