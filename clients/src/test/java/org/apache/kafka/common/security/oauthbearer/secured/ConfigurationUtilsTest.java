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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

public class ConfigurationUtilsTest extends OAuthBearerTest {

    private final static String URI_CONFIG_NAME = "uri";

    @Test
    public void testSSLClientConfig() {
        Map<String, Object> configs = new HashMap<>();
        String sslKeystore = "test.keystore.jks";
        String sslTruststore = "test.truststore.jks";
        String url = "https://www.example.com";

        configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystore);
        configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "$3cr3+");
        configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststore);
        configs.put(URI_CONFIG_NAME, url);

        ConfigurationUtils cu = new ConfigurationUtils(configs);
        Map<String, ?> sslClientConfig = cu.getSslClientConfig(URI_CONFIG_NAME);
        assertNotNull(sslClientConfig);
        assertEquals(sslKeystore, sslClientConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals(sslTruststore, sslClientConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, sslClientConfig.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    @Test
    public void testSSLClientConfigUriEmptyNullAndWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig(""), "required");
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig(null), "required");
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig("  "), "required");
    }

    @Test
    public void testSSLClientConfigUriMalformed() {
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig("not.a.valid.url.com"), "not a valid");
    }

    @Test
    public void testSSLClientConfigUriNotHttps() {
        Map<String, ?> sslClientConfig = getSslClientConfig("http://example.com");
        assertNull(sslClientConfig);
    }

    private Map<String, ?> getSslClientConfig(String uri) {
        Map<String, String> configs = Collections.singletonMap(URI_CONFIG_NAME, uri);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        return cu.getSslClientConfig(URI_CONFIG_NAME);
    }

    @Test
    public void testUri() {
        testUri("http://www.example.com");
    }

    @Test
    public void testUriWithSuperfluousWhitespace() {
        testUri(String.format("  %s  ", "http://www.example.com"));
    }

    @Test
    public void testUriCaseInsensitivity() {
        testUri("HTTPS://WWW.EXAMPLE.COM");
    }

    @Test
    public void testUriFile() {
        testUri("file:///tmp/foo.txt");
    }

    @Test
    public void testUriFullPath() {
        testUri("https://myidp.example.com/oauth2/default/v1/token");
    }

    @Test
    public void testUriMissingScheme() {
        assertThrowsWithMessage(ConfigException.class, () -> testUri("www.example.com"), "missing the scheme");
    }

    @Test
    public void testUriInvalidScheme() {
        assertThrowsWithMessage(ConfigException.class, () -> testUri("ftp://ftp.example.com"), "invalid scheme");
    }

    @Test
    public void testUriNull() {
        assertThrowsWithMessage(ConfigException.class, () -> testUri(null), "must be non-null");
    }

    @Test
    public void testUriEmptyString() {
        assertThrowsWithMessage(ConfigException.class, () -> testUri(""), "must not contain only whitespace");
    }

    @Test
    public void testUriWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> testUri("    "), "must not contain only whitespace");
    }

    private void testUri(String value) {
        Map<String, Object> configs = Collections.singletonMap(URI_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateUri(URI_CONFIG_NAME);
    }

    @Test
    public void testFile() throws IOException {
        File file = TestUtils.tempFile("some contents!");
        testFile(file.toURI().toString());
    }

    @Test
    public void testFileWithSuperfluousWhitespace() throws IOException {
        File file = TestUtils.tempFile();
        testFile(String.format("  %s  ", file.toURI()));
    }

    @Test
    public void testFileDoesNotExist() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile(new File("/tmp/not/a/real/file.txt").toURI().toString()), "that doesn't exist");
    }

    @Test
    public void testFileUnreadable() throws IOException {
        File file = TestUtils.tempFile();

        if (!file.setReadable(false))
            throw new IllegalStateException(String.format("Can't test file permissions as test couldn't programmatically make temp file %s un-readable", file.getAbsolutePath()));

        assertThrowsWithMessage(ConfigException.class, () -> testFile(file.toURI().toString()), "that doesn't have read permission");
    }

    @Test
    public void testFileNull() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile(null), "must be non-null");
    }

    @Test
    public void testFileEmptyString() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile(""), "must not contain only whitespace");
    }

    @Test
    public void testFileWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> testFile("    "), "must not contain only whitespace");
    }

    protected void testFile(String value) {
        Map<String, Object> configs = Collections.singletonMap(URI_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateFile(URI_CONFIG_NAME);
    }

}
