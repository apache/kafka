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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

public class ConfigurationUtilsTest extends OAuthBearerTest {

    private final static String URL_CONFIG_NAME = "url";

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
        assertThrowsWithMessage(ConfigException.class, () -> testUrl(null), "must be non-null");
    }

    @Test
    public void testUrlEmptyString() {
        assertThrowsWithMessage(ConfigException.class, () -> testUrl(""), "must not contain only whitespace");
    }

    @Test
    public void testUrlWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> testUrl("    "), "must not contain only whitespace");
    }

    private void testUrl(String value) {
        Map<String, Object> configs = Collections.singletonMap(URL_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateUrl(URL_CONFIG_NAME);
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
        Map<String, Object> configs = Collections.singletonMap(URL_CONFIG_NAME, value);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        cu.validateFile(URL_CONFIG_NAME);
    }

}
