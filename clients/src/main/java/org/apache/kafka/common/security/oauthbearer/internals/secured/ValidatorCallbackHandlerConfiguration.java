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

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.VerificationKeyResolver;

public class ValidatorCallbackHandlerConfiguration extends AbstractConfig {

    public static final String CLOCK_SKEW_CONFIG = "clockSkew";
    private static final Integer CLOCK_SKEW_DEFAULT = 30;
    private static final String CLOCK_SKEW_DOC = "xxx";
    private static final ConfigDef.Validator CLOCK_SKEW_VALIDATOR = ConfigDef.Range.atLeast(0);

    public static final String EXPECTED_AUDIENCE_CONFIG = "expectedAudience";
    private static final String EXPECTED_AUDIENCE_DOC = "xxx";

    public static final String EXPECTED_ISSUER_CONFIG = "expectedIssuer";
    private static final String EXPECTED_ISSUER_DOC = "xxx";

    public static final String JWKS_ENDPOINT_URI_CONFIG = "jwksEndpointUri";
    private static final String JWKS_ENDPOINT_URI_DOC = "xxx";
    private static final ConfigDef.Validator JWKS_ENDPOINT_URI_VALIDATOR = new UriConfigDefValidator("validator", false);

    public static final String JWKS_FILE_CONFIG = "jwksFile";
    private static final String JWKS_FILE_DOC = "xxx";
    private static final ConfigDef.Validator JWKS_FILE_VALIDATOR = (name, value) -> {
        if (value == null)
            return;

        File file = new File(value.toString().trim());

        if (!file.exists())
            throw new ConfigException(String.format("The OAuth validator configuration option %s contains a file (%s) that doesn't exist", name, value));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth validator configuration option %s contains a file (%s) that doesn't have read permission", name, value));
    };

    public static final String SCOPE_CLAIM_NAME_CONFIG = "scopeClaimName";
    private static final String SCOPE_CLAIM_NAME_DEFAULT = "scope";
    private static final String SCOPE_CLAIM_NAME_DOC = "xxx";

    public static final String SUB_CLAIM_NAME_CONFIG = "subClaimName";
    private static final String SUB_CLAIM_NAME_DEFAULT = ReservedClaimNames.SUBJECT;
    private static final String SUB_CLAIM_NAME_DOC = "xxx";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(CLOCK_SKEW_CONFIG,
            Type.INT,
            CLOCK_SKEW_DEFAULT,
            CLOCK_SKEW_VALIDATOR,
            Importance.LOW,
            CLOCK_SKEW_DOC)
        .define(EXPECTED_AUDIENCE_CONFIG,
            Type.LIST,
            null,
            Importance.LOW,
            EXPECTED_AUDIENCE_DOC)
        .define(EXPECTED_ISSUER_CONFIG,
            Type.STRING,
            null,
            Importance.LOW,
            EXPECTED_ISSUER_DOC)
        .define(JWKS_ENDPOINT_URI_CONFIG,
            Type.STRING,
            null,
            JWKS_ENDPOINT_URI_VALIDATOR,
            Importance.MEDIUM,
            JWKS_ENDPOINT_URI_DOC)
        .define(JWKS_FILE_CONFIG,
            Type.STRING,
            null,
            JWKS_FILE_VALIDATOR,
            Importance.MEDIUM,
            JWKS_FILE_DOC)
        .define(SCOPE_CLAIM_NAME_CONFIG,
            Type.STRING,
            SCOPE_CLAIM_NAME_DEFAULT,
            Importance.LOW,
            SCOPE_CLAIM_NAME_DOC)
        .define(SUB_CLAIM_NAME_CONFIG,
            Type.STRING,
            SUB_CLAIM_NAME_DEFAULT,
            Importance.LOW,
            SUB_CLAIM_NAME_DOC)
        ;

    private final Set<String> expectedAudiences;

    private final VerificationKeyResolver verificationKeyResolver;

    public ValidatorCallbackHandlerConfiguration(Map<String, ?> options) {
        super(CONFIG, options);

        if (options.containsKey(EXPECTED_AUDIENCE_CONFIG)) {
            List<String> tmp = getList(EXPECTED_AUDIENCE_CONFIG);

            if (tmp != null)
                expectedAudiences = Collections.unmodifiableSet(new HashSet<>(tmp));
            else
                expectedAudiences = null;
        } else {
            expectedAudiences = null;
        }

        String jwksFile = getString(JWKS_FILE_CONFIG);
        String jwksEndpointUri = getString(JWKS_ENDPOINT_URI_CONFIG);

        if (jwksFile != null && jwksEndpointUri != null)
            throw new ConfigException(String.format("The OAuth validator configuration options %s and %s cannot both be specified", JWKS_FILE_CONFIG, JWKS_ENDPOINT_URI_CONFIG));

        if (jwksFile != null) {
            // TODO
            verificationKeyResolver = null;
        } else if (jwksEndpointUri != null) {
            HttpsJwks httpsJkws = new HttpsJwks(jwksEndpointUri);
            verificationKeyResolver = new HttpsJwksVerificationKeyResolver(httpsJkws);
        } else {
            throw new ConfigException(String.format("The OAuth validator configuration must include either %s or %s options", JWKS_FILE_CONFIG, JWKS_ENDPOINT_URI_CONFIG));
        }
    }

    public Integer getClockSkew() {
        return getInt(CLOCK_SKEW_CONFIG);
    }

    public Set<String> getExpectedAudiences() {
        return expectedAudiences;
    }

    public String getExpectedIssuer() {
        return getString(EXPECTED_ISSUER_CONFIG);
    }

    public String getScopeClaimName() {
        return getString(SCOPE_CLAIM_NAME_CONFIG);
    }

    public String getSubClaimName() {
        return getString(SUB_CLAIM_NAME_CONFIG);
    }

    public VerificationKeyResolver getVerificationKeyResolver() {
        return verificationKeyResolver;
    }

}
