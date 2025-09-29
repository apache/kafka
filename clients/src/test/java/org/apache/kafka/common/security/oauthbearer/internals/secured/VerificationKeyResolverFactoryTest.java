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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;

public class VerificationKeyResolverFactoryTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolver() throws Exception {
        File tmpDir = createTempDir("access-token");
        File verificationKeyFile = createTempFile(tmpDir, "access-token-", ".json", "{}");

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, verificationKeyFile.toURI().toString());
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, verificationKeyFile.toURI().toString());
        Map<String, Object> jaasConfig = Collections.emptyMap();

        // verify it won't throw exception
        try (CloseableVerificationKeyResolver verificationKeyResolver = VerificationKeyResolverFactory.create(configs, jaasConfig)) { }
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolverWithInvalidDirectory() {
        // Should fail because the parent path doesn't exist.
        String file = new File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        Map<String, Object> jaasConfig = Collections.emptyMap();
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, jaasConfig), "that doesn't exist");
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolverWithInvalidFile() throws Exception {
        // Should fail because while the parent path exists, the file itself doesn't.
        File tmpDir = createTempDir("this-directory-does-exist");
        File verificationKeyFile = new File(tmpDir, "this-file-does-not-exist.json");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, verificationKeyFile.toURI().toString());
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, verificationKeyFile.toURI().toString());
        Map<String, Object> jaasConfig = Collections.emptyMap();
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, jaasConfig), "that doesn't exist");
    }

    @Test
    public void testSaslOauthbearerTokenEndpointUrlIsNotAllowed() throws Exception {
        // Should fail if the URL is not allowed
        File tmpDir = createTempDir("not_allowed");
        File verificationKeyFile = new File(tmpDir, "not_allowed.json");
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, verificationKeyFile.toURI().toString());
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, Collections.emptyMap()),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }
}
