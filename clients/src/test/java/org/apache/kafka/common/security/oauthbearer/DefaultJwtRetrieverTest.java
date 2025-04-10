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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultJwtRetrieverTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testConfigureRefreshingFileJwtRetriever() throws Exception {
        String expected = "{}";

        File jwtFile = createTempFile("jwt-", ".json", expected);

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, jwtFile.toURI().toString());
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, jwtFile.toURI().toString());

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, List.of());
            assertEquals(expected, jwtRetriever.retrieve());
        }
    }

    @Test
    public void testConfigureRefreshingFileJwtRetrieverWithInvalidDirectory() {
        // Should fail because the parent path doesn't exist.
        String file = new File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, file);

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertThrowsWithMessage(ConfigException.class, () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, List.of()), "that doesn't exist");
        }
    }

    @Test
    public void testConfigureRefreshingFileJwtRetrieverWithInvalidFile() throws Exception {
        // Should fail because while the parent path exists, the file itself doesn't.
        File jwtFile = new File(TestUtils.tempDirectory(), "this-file-does-not-exist.json");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, jwtFile.toURI().toString());
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, jwtFile.toURI().toString());

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertThrowsWithMessage(ConfigException.class, () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, List.of()), "that doesn't exist");
        }
    }

    @Test
    public void testSaslOauthbearerTokenEndpointUrlIsNotAllowed() throws Exception {
        // Should fail if the URL is not allowed
        File jwtFile = new File("not_allowed.json");
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, jwtFile.toURI().toString());

        try (JwtRetriever jwtRetriever = new DefaultJwtRetriever()) {
            assertThrowsWithMessage(ConfigException.class, () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, List.of()), ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        }
    }
}
