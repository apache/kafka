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

package org.apache.kafka.common.security.oauthbearer.secured;

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

/**
 * Configuration for the validator portion of the login/validation authentication process for
 * OAuth/OIDC that will be executed on the broker.
 *
 * @see LoginCallbackHandlerConfiguration for the client login configuration
 */

public class ValidatorCallbackHandlerConfiguration extends AbstractConfig {

    public static final String CLOCK_SKEW_CONFIG = "clockSkew";
    public static final String EXPECTED_AUDIENCE_CONFIG = "expectedAudience";
    public static final String EXPECTED_ISSUER_CONFIG = "expectedIssuer";
    public static final String JWKS_ENDPOINT_REFRESH_INTERVAL_MS_CONFIG = "jwksEndpointRefreshIntervalMs";
    public static final String JWKS_ENDPOINT_URI_CONFIG = "jwksEndpointUri";
    public static final String JWKS_FILE_CONFIG = "jwksFile";
    public static final String SCOPE_CLAIM_NAME_CONFIG = "scopeClaimName";
    public static final String SUB_CLAIM_NAME_CONFIG = "subClaimName";

    private static final int CLOCK_SKEW_DEFAULT = 30;
    private static final String CLOCK_SKEW_DOC = "The (optional) value in " +
        "seconds to allow for differences between the time of the OAuth/OIDC identity " +
        "provider and the broker.";
    private static final ConfigDef.Validator CLOCK_SKEW_VALIDATOR = new OptionalNumberConfigDefValidator(0, false);

    private static final String EXPECTED_AUDIENCE_DOC = "The (optional) comma-delimited " +
        "setting for the broker to use to verify that the JWT was issued for one of the " +
        "expected audiences. The JWT will be inspected for the standard OAuth \"aud\" claim " +
        "and if this value is set, the broker will match the value from JWT's \"aud\" claim " +
        "to see if there is an exact match. If there is no match, the broker will reject " +
        "the JWT and authentication will fail.";

    private static final String EXPECTED_ISSUER_DOC = "The (optional) setting for " +
        "the broker to use to verify that the JWT was created by the expected issuer. " +
        "The JWT will be inspected for the standard OAuth \"iss\" claim and if this value " +
        "is set, the broker will match it exactly against what is in the JWT's \"iss\" claim. " +
        "If there is no match, the broker will reject the JWT and authentication will fail.";

    private static final long JWKS_ENDPOINT_REFRESH_INTERVAL_MS_DEFAULT = 60 * 60 * 1000;
    private static final String JWKS_ENDPOINT_REFRESH_INTERVAL_MS_DOC = "The (optional) value in " +
        "milliseconds for the broker to wait between refreshing its JWKS (JSON Web Key Set) " +
        "cache that contains the keys to verify the signature of the JWT.";
    private static final ConfigDef.Validator JWKS_ENDPOINT_REFRESH_INTERVAL_MS_VALIDATOR = new OptionalNumberConfigDefValidator(30 * 1000, false);

    private static final String JWKS_ENDPOINT_URI_DOC = "The OAuth/OIDC provider URI from " +
        "which the provider's JWKS (JSON Web Key Set) can be retrieved." +
        ""  +
        "Note: only one of " + JWKS_ENDPOINT_URI_CONFIG + " or " + JWKS_FILE_CONFIG + " should " +
        "be configured as they are mutually exclusive. An error will be generated at broker " +
        "start if both are provided. " +
        ""  +
        "In this mode, the JWKS data will be retrieved from the " +
        "OAuth/OIDC provider via the configured URI on broker startup. All then-current " +
        "keys will be cached on the broker for incoming requests. If an authentication " +
        "request is received for a JWT that includes a \"kid\" header claim value that " +
        "isn't yet in the cache, the JWKS endpoint will be queried again on demand. " +
        "However, the broker polls the URI every " + JWKS_ENDPOINT_REFRESH_INTERVAL_MS_CONFIG +
        "milliseconds to refresh the cache with any forthcoming keys before any " +
        "JWT requests that include them are received.";
    private static final ConfigDef.Validator JWKS_ENDPOINT_URI_VALIDATOR = new UriConfigDefValidator();

    private static final String JWKS_FILE_DOC = "The file name of the file that contains a " +
        "copy of the OAuth/OIDC provider's JWKS (JSON Web Key Set)." +
        ""  +
        "Note: only one of " + JWKS_ENDPOINT_URI_CONFIG + " or " + JWKS_FILE_CONFIG + " should " +
        "be configured as they are mutually exclusive. An error will be generated at broker " +
        "start if both are provided. " +
        ""  +
        "In this mode, the broker will load the JWKS file from a configured location " +
        "on startup and will watch the file for updates which allows for dynamic " +
        "configuration updates. The means by which the JWKS file is updated is left to the " +
        "broker administrator. In the event that the JWT includes a \"kid\" header claim " +
        "value that isn't in the file is encountered, the broker will reject the JWT and " +
        "authentication will fail.";
    private static final ConfigDef.Validator JWKS_FILE_VALIDATOR = new FileConfigDefValidator();

    private static final String SCOPE_CLAIM_NAME_DEFAULT = "scope";
    private static final String SCOPE_CLAIM_NAME_DOC = "The OAuth claim for the scope is often " +
        "named \"" + SCOPE_CLAIM_NAME_DEFAULT + "\", but this (optional) setting can provide " +
        "a different name to use for the scope included in the JWT payload's claims if the " +
        "OAuth/OIDC provider uses a different name for that claim.";

    private static final String SUB_CLAIM_NAME_DEFAULT = ReservedClaimNames.SUBJECT;
    private static final String SUB_CLAIM_NAME_DOC = "The OAuth claim for the subject is often " +
        "named \"" + SUB_CLAIM_NAME_DEFAULT + "\", but this (optional) setting can provide " +
        "a different name to use for the subject included in the JWT payload's claims if the " +
        "OAuth/OIDC provider uses a different name for that claim.";

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
        .define(JWKS_ENDPOINT_REFRESH_INTERVAL_MS_CONFIG,
            Type.LONG,
            JWKS_ENDPOINT_REFRESH_INTERVAL_MS_DEFAULT,
            JWKS_ENDPOINT_REFRESH_INTERVAL_MS_VALIDATOR,
            Importance.LOW,
            JWKS_ENDPOINT_REFRESH_INTERVAL_MS_DOC)
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
            SUB_CLAIM_NAME_DOC);

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

        if (jwksFile != null && jwksEndpointUri != null) {
            throw new ConfigException(String.format("The OAuth validator configuration options %s and %s cannot both be specified", JWKS_FILE_CONFIG, JWKS_ENDPOINT_URI_CONFIG));
        } else if (jwksFile == null && jwksEndpointUri == null) {
            throw new ConfigException(String.format("The OAuth validator configuration must include either %s or %s options", JWKS_FILE_CONFIG, JWKS_ENDPOINT_URI_CONFIG));
        } else if (jwksFile != null) {
            // TODO
            verificationKeyResolver = null;
            throw new UnsupportedOperationException("Not yet implemented");
        } else {
            long refreshIntervalMs = getLong(JWKS_ENDPOINT_REFRESH_INTERVAL_MS_CONFIG);
            HttpsJwks httpsJkws = new RefreshingHttpsJwks(jwksEndpointUri, refreshIntervalMs);
            verificationKeyResolver = new HttpsJwksVerificationKeyResolver(httpsJkws);
        }
    }

    public ValidatorCallbackHandlerConfiguration(Map<String, ?> options,
        Set<String> expectedAudiences,
        VerificationKeyResolver verificationKeyResolver) {
        super(CONFIG, options);

        this.expectedAudiences = expectedAudiences;
        this.verificationKeyResolver = verificationKeyResolver;
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
