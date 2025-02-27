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

import org.jose4j.keys.resolvers.VerificationKeyResolver;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;

public class AccessTokenValidatorFactory {

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
        Set<String> expectedAudiences = null;
        List<String> l = cu.get(SASL_OAUTHBEARER_EXPECTED_AUDIENCE);

        if (l != null)
            expectedAudiences = Set.copyOf(l);

        Integer clockSkew = cu.validateInteger(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, false);
        String expectedIssuer = cu.validateString(SASL_OAUTHBEARER_EXPECTED_ISSUER, false);
        String scopeClaimName = cu.validateString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        String subClaimName = cu.validateString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);

        return new ValidatorAccessTokenValidator(clockSkew,
            expectedAudiences,
            expectedIssuer,
            verificationKeyResolver,
            scopeClaimName,
            subClaimName);
    }

}
