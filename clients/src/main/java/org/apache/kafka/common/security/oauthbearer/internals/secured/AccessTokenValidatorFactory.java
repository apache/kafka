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

import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;

public class AccessTokenValidatorFactory {

    private static final Logger log = LoggerFactory.getLogger(AccessTokenValidatorFactory.class);

    public static AccessTokenValidator create(Map<String, ?> configs) {
        return create(configs, (String) null);
    }

    public static AccessTokenValidator create(Map<String, ?> configs, String saslMechanism) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        String scopeClaimName = cu.get(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        String subClaimName = cu.get(SASL_OAUTHBEARER_SUB_CLAIM_NAME);
        return new LoginAccessTokenValidator(scopeClaimName, subClaimName);
    }

    public static AccessTokenValidator create(Map<String, ?> configs,
        VerificationKeyResolver verificationKeyResolver) {
        return create(configs, null, verificationKeyResolver);
    }

    public static AccessTokenValidator create(Map<String, ?> configs,
        String saslMechanism,
        VerificationKeyResolver verificationKeyResolver) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        Set<String> expectedAudiences = Collections.emptySet();
        List<String> l = cu.get(SASL_OAUTHBEARER_EXPECTED_AUDIENCE);

        if (l != null)
            expectedAudiences = Collections.unmodifiableSet(new HashSet<>(l));

        Integer clockSkew = cu.validateInteger(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, false);
        String expectedIssuer = cu.validateString(SASL_OAUTHBEARER_EXPECTED_ISSUER, false);
        String scopeClaimName = cu.validateString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        String subClaimName = cu.validateString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);
        boolean allowUnverifiedAudience = Boolean.TRUE.equals(cu.validateBoolean(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE, false));
        boolean allowUnverifiedIssuer = Boolean.TRUE.equals(cu.validateBoolean(SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER, false));

        // The broker must be configured with at least one expected audience so it can verify that the token was minted
        // for this cluster. Without it, a token issued for any other service by the same provider would be accepted.
        // Fail fast at configuration time unless the operator has explicitly opted out.
        if (expectedAudiences.isEmpty() && !allowUnverifiedAudience) {
            throw new ConfigException(String.format(
                "The OAuth validator for the broker requires \"%s\" to be configured so that the token's audience" +
                " (\"aud\") claim can be verified, but it was not set. Set \"%s\" to the audience(s) that identify this" +
                " cluster, or set \"%s\" to true to accept tokens regardless of their audience.",
                SASL_OAUTHBEARER_EXPECTED_AUDIENCE, SASL_OAUTHBEARER_EXPECTED_AUDIENCE, SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE));
        }

        // The broker must be configured with an expected issuer. Without it, jose4j accepts a token bearing any (or no)
        // "iss" claim, so the issuer cannot be verified. Fail fast at configuration time unless the operator has
        // explicitly opted out.
        if (expectedIssuer == null && !allowUnverifiedIssuer) {
            throw new ConfigException(String.format(
                "The OAuth validator for the broker requires \"%s\" to be configured so that the token's issuer" +
                " (\"iss\") claim can be verified, but it was not set. Set \"%s\" to the issuer URL of the OAuth/OIDC" +
                " provider that signs the tokens, or set \"%s\" to true to accept tokens regardless of their issuer.",
                SASL_OAUTHBEARER_EXPECTED_ISSUER, SASL_OAUTHBEARER_EXPECTED_ISSUER, SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER));
        }

        // Reached only when the corresponding opt-out is enabled (otherwise configuration fails fast above).
        if (expectedAudiences.isEmpty()) {
            log.warn("The OAuth broker validator is configured without \"{}\", and \"{}\" is set to true, so it will" +
                " accept a JWT regardless of its \"aud\" (audience) claim.",
                SASL_OAUTHBEARER_EXPECTED_AUDIENCE, SASL_OAUTHBEARER_ALLOW_UNVERIFIED_AUDIENCE);
        }

        if (expectedIssuer == null) {
            log.warn("The OAuth broker validator is configured without \"{}\", and \"{}\" is set to true, so it will" +
                " accept a JWT bearing any (or no) \"iss\" (issuer) claim.",
                SASL_OAUTHBEARER_EXPECTED_ISSUER, SASL_OAUTHBEARER_ALLOW_UNVERIFIED_ISSUER);
        }

        return new ValidatorAccessTokenValidator(clockSkew,
            expectedAudiences,
            expectedIssuer,
            allowUnverifiedAudience,
            verificationKeyResolver,
            scopeClaimName,
            subClaimName);
    }

}
