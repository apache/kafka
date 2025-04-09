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

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.maybeCreateSslResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JaasOptionsUtilsTest extends OAuthBearerTest {

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

}
