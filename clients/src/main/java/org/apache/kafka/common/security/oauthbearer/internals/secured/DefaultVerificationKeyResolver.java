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

import org.apache.kafka.common.utils.Utils;

import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.lang.UnresolvableKeyException;

import java.io.IOException;
import java.net.URL;
import java.security.Key;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.protocolMatches;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.requireConfigured;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

public class DefaultVerificationKeyResolver implements CloseableVerificationKeyResolver {

    private CloseableVerificationKeyResolver delegate;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig config = new OAuthBearerConfig(configs, saslMechanism);
        URL jwksEndpointUrl = validateUrl(config, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);

        if (protocolMatches(jwksEndpointUrl, "file")) {
            delegate = new JwksFileVerificationKeyResolver();
        } else {
            delegate = new RefreshingHttpsJwksVerificationKeyResolver();
        }

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        return requireConfigured(delegate, () -> "Verification key resolver delegate", getClass()).resolveKey(jws, nestingContext);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "Verification key resolver delegate");
    }

    CloseableVerificationKeyResolver delegate() {
        return delegate;
    }
}