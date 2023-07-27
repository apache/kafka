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
package org.apache.kafka.connect.runtime.rest;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.rest.RestServerConfig.LISTENERS_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RestServerConfigTest {

    private static final List<String> VALID_HEADER_CONFIGS = Arrays.asList(
            "add \t Cache-Control: no-cache, no-store, must-revalidate",
            "add \r X-XSS-Protection: 1; mode=block",
            "\n add Strict-Transport-Security: max-age=31536000; includeSubDomains",
            "AdD   Strict-Transport-Security:  \r  max-age=31536000;  includeSubDomains",
            "AdD \t Strict-Transport-Security : \n   max-age=31536000;  includeSubDomains",
            "add X-Content-Type-Options: \r nosniff",
            "Set \t X-Frame-Options: \t Deny\n ",
            "seT \t X-Cache-Info: \t not cacheable\n ",
            "seTDate \t Expires: \r 31540000000",
            "adDdate \n Last-Modified: \t 0"
    );

    private static final List<String> INVALID_HEADER_CONFIGS = Arrays.asList(
            "set \t",
            "badaction \t X-Frame-Options:DENY",
            "set add X-XSS-Protection:1",
            "addX-XSS-Protection",
            "X-XSS-Protection:",
            "add set X-XSS-Protection: 1",
            "add X-XSS-Protection:1 X-XSS-Protection:1 ",
            "add X-XSS-Protection",
            "set X-Frame-Options:DENY, add  :no-cache, no-store, must-revalidate "
    );

    @Test
    public void testListenersConfigAllowedValues() {
        Map<String, String> props = new HashMap<>();

        // no value set for "listeners"
        RestServerConfig config = RestServerConfig.forPublic(null, props);
        assertEquals(LISTENERS_DEFAULT, config.listeners());

        props.put(RestServerConfig.LISTENERS_CONFIG, "http://a.b:9999");
        config = RestServerConfig.forPublic(null, props);
        assertEquals(Arrays.asList("http://a.b:9999"), config.listeners());

        props.put(RestServerConfig.LISTENERS_CONFIG, "http://a.b:9999, https://a.b:7812");
        config = RestServerConfig.forPublic(null, props);
        assertEquals(Arrays.asList("http://a.b:9999", "https://a.b:7812"), config.listeners());

        config = RestServerConfig.forPublic(null, props);
    }

    @Test
    public void testListenersConfigNotAllowedValues() {
        Map<String, String> props = new HashMap<>();
        assertEquals(LISTENERS_DEFAULT, RestServerConfig.forPublic(null, props).listeners());

        props.put(RestServerConfig.LISTENERS_CONFIG, "");
        ConfigException ce = assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
        assertTrue(ce.getMessage().contains(" listeners"));

        props.put(RestServerConfig.LISTENERS_CONFIG, ",,,");
        ce = assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
        assertTrue(ce.getMessage().contains(" listeners"));

        props.put(RestServerConfig.LISTENERS_CONFIG, "http://a.b:9999,");
        ce = assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
        assertTrue(ce.getMessage().contains(" listeners"));

        props.put(RestServerConfig.LISTENERS_CONFIG, "http://a.b:9999, ,https://a.b:9999");
        ce = assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
        assertTrue(ce.getMessage().contains(" listeners"));
    }

    @Test
    public void testAdminListenersConfigAllowedValues() {
        Map<String, String> props = new HashMap<>();

        // no value set for "admin.listeners"
        RestServerConfig config = RestServerConfig.forPublic(null, props);
        assertNull("Default value should be null.", config.adminListeners());

        props.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "");
        config = RestServerConfig.forPublic(null, props);
        assertTrue(config.adminListeners().isEmpty());

        props.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999, https://a.b:7812");
        config = RestServerConfig.forPublic(null, props);
        assertEquals(Arrays.asList("http://a.b:9999", "https://a.b:7812"), config.adminListeners());

        RestServerConfig.forPublic(null, props);
    }

    @Test
    public void testAdminListenersNotAllowingEmptyStrings() {
        Map<String, String> props = new HashMap<>();

        props.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999,");
        ConfigException ce = assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
        assertTrue(ce.getMessage().contains(" admin.listeners"));
    }

    @Test
    public void testAdminListenersNotAllowingBlankStrings() {
        Map<String, String> props = new HashMap<>();
        props.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "http://a.b:9999, ,https://a.b:9999");
        assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
    }

    @Test
    public void testInvalidHeaderConfigs() {
        for (String config : INVALID_HEADER_CONFIGS) {
            assertInvalidHeaderConfig(config);
        }
    }

    @Test
    public void testValidHeaderConfigs() {
        for (String config : VALID_HEADER_CONFIGS) {
            assertValidHeaderConfig(config);
        }
    }

    private void assertInvalidHeaderConfig(String config) {
        assertThrows(ConfigException.class, () -> RestServerConfig.validateHttpResponseHeaderConfig(config));
    }

    private void assertValidHeaderConfig(String config) {
        RestServerConfig.validateHttpResponseHeaderConfig(config);
    }

    @Test
    public void testInvalidSslClientAuthConfig() {
        Map<String, String> props = new HashMap<>();

        props.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "abc");
        ConfigException ce = assertThrows(ConfigException.class, () -> RestServerConfig.forPublic(null, props));
        assertTrue(ce.getMessage().contains(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));
    }
}
