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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

public class ConfigurationUtilsTest extends OAuthBearerTest {

    private final static String URI_CONFIG_NAME = "uri";

    @Test
    public void testSSLClientConfig() {
        Map<String, String> config = new HashMap<>();
        String sslKeystore = "test.keystore.jks";
        String sslTruststore = "test.truststore.jks";
        String url = "https://www.example.com";

        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystore);
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "$3cr3+");
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststore);
        config.put(URI_CONFIG_NAME, url);

        Map<String, ?> sslClientConfig = ConfigurationUtils.getSslClientConfig(config, URI_CONFIG_NAME);
        assertNotNull(sslClientConfig);
        assertEquals(sslKeystore, sslClientConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals(sslTruststore, sslClientConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, sslClientConfig.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    @Test
    public void testUriEmptyNullAndWhitespace() {
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig(""), "required");
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig(null), "required");
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig("  "), "required");
    }

    @Test
    public void testUriMalformed() {
        assertThrowsWithMessage(ConfigException.class, () -> getSslClientConfig("not.a.valid.url.com"), "not a valid");
    }

    @Test
    public void testUriNotHttps() {
        Map<String, ?> sslClientConfig = getSslClientConfig("http://example.com");
        assertNull(sslClientConfig);
    }

    private Map<String, ?> getSslClientConfig(String uri) {
        Map<String, String> config = Collections.singletonMap(URI_CONFIG_NAME, uri);
        return ConfigurationUtils.getSslClientConfig(config, URI_CONFIG_NAME);
    }

}
