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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.keys.resolvers.VerificationKeyResolver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * This {@link JwtValidator} uses the delegation approach, instantiating and delegating calls to a
 * more concrete implementation. The underlying implementation is determined by the presence/absence
 * of the {@link VerificationKeyResolver}: if it's present, a {@link BrokerJwtValidator} is
 * created, otherwise a {@link ClientJwtValidator} is created.
 */
public class DefaultJwtValidator implements JwtValidator {

    private final Optional<CloseableVerificationKeyResolver> verificationKeyResolver;

    private JwtValidator delegate;

    public DefaultJwtValidator() {
        this.verificationKeyResolver = Optional.empty();
    }

    public DefaultJwtValidator(CloseableVerificationKeyResolver verificationKeyResolver) {
        this.verificationKeyResolver = Optional.of(verificationKeyResolver);
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (verificationKeyResolver.isPresent()) {
            delegate = new BrokerJwtValidator(verificationKeyResolver.get());
        } else {
            ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);

            if (cu.containsKey(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL)) {
                delegate = new BrokerJwtValidator();
            } else {
                delegate = new ClientJwtValidator();
            }
        }

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
        if (delegate == null)
            throw new IllegalStateException("JWT validator delegate is null; please call configure() first");

        return delegate.validate(accessToken);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "JWT validator delegate");
    }

    JwtValidator delegate() {
        return delegate;
    }
}
