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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultJwtValidatorTest extends OAuthBearerTest {

    @Test
    public void testConfigureWithVerificationKeyResolver() throws IOException {
        String url = "http://www.example.com/";
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, url);
        Map<String, ?> configs = getSaslConfigs(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, url);

        try (DefaultJwtValidator jwtValidator = new DefaultJwtValidator()) {
            assertThrows(ConfigException.class, () -> jwtValidator.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfig()));
            assertInstanceOf(BrokerJwtValidator.class, jwtValidator.delegate());
        }
    }

    @Test
    public void testConfigureWithoutVerificationKeyResolver() throws IOException {
        Map<String, ?> configs = getSaslConfigs();

        try (DefaultJwtValidator jwtValidator = new DefaultJwtValidator()) {
            assertDoesNotThrow(() -> jwtValidator.configure(configs, OAUTHBEARER_MECHANISM, List.of()));
            assertInstanceOf(ClientJwtValidator.class, jwtValidator.delegate());
        }
    }
}
