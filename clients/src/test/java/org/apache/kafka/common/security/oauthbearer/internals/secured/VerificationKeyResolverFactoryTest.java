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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogCaptureAppender;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VerificationKeyResolverFactoryTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolver() throws Exception {
        String file = tempFile("{}").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()), "The JSON JWKS content does not include the keys member");
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolverWithInvalidDirectory() {
        // Should fail because the parent path doesn't exist.
        String file = new File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()), "that doesn't exist");
    }

    @Test
    public void testSaslOauthbearerTokenEndpointUrlIsNotAllowed() throws Exception {
        // Should fail if the URL is not allowed
        String file = tempFile("{}").toURI().toString();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testHttpJwksEndpointUrlLogsWarning() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        String httpUrl = "http://localhost:" + port + "/jwks";
        String httpsUrl = "https://localhost:" + port + "/jwks";
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, httpUrl + "," + httpsUrl);

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(VerificationKeyResolverFactory.class)) {
            appender.setClassLogger(VerificationKeyResolverFactory.class, Level.WARN);
            assertThrows(KafkaException.class, () -> VerificationKeyResolverFactory.create(
                getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, httpsUrl), OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
            assertEquals(List.of(), appender.getMessages(Level.WARN));

            assertThrows(KafkaException.class, () -> VerificationKeyResolverFactory.create(
                getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, httpUrl), OAUTHBEARER_MECHANISM, getJaasConfigEntries()));
            assertEquals(List.of("The sasl.oauthbearer.jwks.endpoint.url configuration uses an HTTP URL, so the keys used to verify " +
                "tokens are retrieved without TLS. Use an HTTPS URL instead."), appender.getMessages(Level.WARN));
        }
    }
}
