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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.keys.resolvers.VerificationKeyResolver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;

public class DefaultJwtValidator implements JwtValidator {

    private final Map<String, ?> configs;
    private final String saslMechanism;
    private final Optional<VerificationKeyResolver> verificationKeyResolver;

    private JwtValidator delegate;

    public DefaultJwtValidator(Map<String, ?> configs, String saslMechanism) {
        this.configs = configs;
        this.saslMechanism = saslMechanism;
        this.verificationKeyResolver = Optional.empty();
    }

    public DefaultJwtValidator(Map<String, ?> configs,
                               String saslMechanism,
                               VerificationKeyResolver verificationKeyResolver) {
        this.configs = configs;
        this.saslMechanism = saslMechanism;
        this.verificationKeyResolver = Optional.of(verificationKeyResolver);
    }

    @Override
    public void init() throws IOException {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);

        if (verificationKeyResolver.isPresent()) {
            Set<String> expectedAudiences = null;
            List<String> l = cu.get(SASL_OAUTHBEARER_EXPECTED_AUDIENCE);

            if (l != null)
                expectedAudiences = Set.copyOf(l);

            Integer clockSkew = cu.validateInteger(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, false);
            String expectedIssuer = cu.validateString(SASL_OAUTHBEARER_EXPECTED_ISSUER, false);
            String scopeClaimName = cu.validateString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
            String subClaimName = cu.validateString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);

            delegate = new BrokerJwtValidator(clockSkew,
                expectedAudiences,
                expectedIssuer,
                verificationKeyResolver.get(),
                scopeClaimName,
                subClaimName);
        } else {
            String scopeClaimName = cu.get(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
            String subClaimName = cu.get(SASL_OAUTHBEARER_SUB_CLAIM_NAME);
            delegate = new ClientJwtValidator(scopeClaimName, subClaimName);
        }

        delegate.init();
    }

    @Override
    public OAuthBearerToken validate(String accessToken) throws ValidateException {
        return Objects.requireNonNull(delegate).validate(accessToken);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "delegate");
    }

    public JwtValidator delegate() {
        return delegate;
    }
}
