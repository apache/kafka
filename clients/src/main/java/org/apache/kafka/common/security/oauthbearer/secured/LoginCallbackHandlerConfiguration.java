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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.CompositeValidator;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Configuration for the login portion of the login/validation authentication process for
 * OAuth/OIDC that will be executed on the client.
 *
 * @see ValidatorCallbackHandlerConfiguration for the broker validator configuration
 */

public class LoginCallbackHandlerConfiguration extends AbstractConfig {

    public static final String ACCESS_TOKEN_CONFIG = "accessToken";
    public static final String ACCESS_TOKEN_FILE_CONFIG = "accessTokenFile";
    public static final String CLIENT_ID_CONFIG = "clientId";
    public static final String CLIENT_SECRET_CONFIG = "clientSecret";
    public static final String LOGIN_ATTEMPTS_CONFIG = "loginAttempts";
    public static final String LOGIN_CONNECT_TIMEOUT_MS_CONFIG = "loginConnectTimeoutMs";
    public static final String LOGIN_READ_TIMEOUT_MS_CONFIG = "loginReadTimeoutMs";
    public static final String LOGIN_RETRY_MAX_WAIT_MS_CONFIG = "loginRetryMaxWaitMs";
    public static final String LOGIN_RETRY_WAIT_MS_CONFIG = "loginRetryWaitMs";
    public static final String SCOPE_CONFIG = "scope";
    public static final String SCOPE_CLAIM_NAME_CONFIG = "scopeClaimName";
    public static final String SUB_CLAIM_NAME_CONFIG = "subClaimName";
    public static final String TOKEN_ENDPOINT_URI_CONFIG = "tokenEndpointUri";

    private static final String ACCESS_TOKEN_RETRIEVER_NOTE =
        "Note: only one of " + ACCESS_TOKEN_CONFIG + ", " + ACCESS_TOKEN_FILE_CONFIG + ", or " +
            CLIENT_ID_CONFIG + " should be configured as they are mutually exclusive. " +
            "An error will be generated at client start if more than one is provided.";

    private static final String ACCESS_TOKEN_DOC = "An access token " +
        "(in JWT serialized form) issued by the OAuth/OIDC identity provider to use for " +
        "authorization for this client." +
        " "  + ACCESS_TOKEN_RETRIEVER_NOTE;

    private static final String ACCESS_TOKEN_FILE_DOC = "A file containing an access token " +
        "(in JWT serialized form) issued by the OAuth/OIDC identity provider to use for " +
        "authorization for this client." +
        " "  + ACCESS_TOKEN_RETRIEVER_NOTE;

    private static final String CLIENT_ID_DOC = "The OAuth/OIDC identity provider-issued " +
        "client ID to uniquely identify the service account to use for authentication for " +
        "this client. The value must be paired with a corresponding " + CLIENT_SECRET_CONFIG + " " +
        "value and is provided to the OAuth provider using the OAuth " +
        "clientcredentials grant type." +
        " "  + ACCESS_TOKEN_RETRIEVER_NOTE;

    private static final String CLIENT_SECRET_DOC = "The OAuth/OIDC identity provider-issued " +
        "client secret serves a similar function as a password to the " + CLIENT_ID_CONFIG + " " +
        "account and identifies the service account to use for authentication for " +
        "this client. The value must be paired with a corresponding " + CLIENT_ID_CONFIG + " " +
        "value and is provided to the OAuth provider using the OAuth " +
        "clientcredentials grant type.";

    private static final int LOGIN_ATTEMPTS_DEFAULT = 3;
    private static final String LOGIN_ATTEMPTS_DOC = "The (optional) number of attempts to " +
        "connect to the OAuth/OIDC identity provider to log in this client. " +
        "The login attempts use an exponential backoff algorithm with an initial " +
        "wait based on the " + LOGIN_RETRY_WAIT_MS_CONFIG + " setting and will double in wait " +
        "length between attempts up to a maximum wait length specified by " +
        LOGIN_RETRY_MAX_WAIT_MS_CONFIG + ".";
    private static final ConfigDef.Validator LOGIN_ATTEMPTS_VALIDATOR = new OptionalNumberConfigDefValidator(1);

    private static final String LOGIN_CONNECT_TIMEOUT_MS_DOC = "The (optional) value in " +
        "milliseconds for the OAuth/OIDC identity provider HTTP/HTTPS connection timeout.";
    private static final ConfigDef.Validator LOGIN_CONNECT_TIMEOUT_MS_VALIDATOR = new OptionalNumberConfigDefValidator(0, false);

    private static final String LOGIN_READ_TIMEOUT_MS_DOC = "The (optional) value in " +
        "milliseconds for the OAuth/OIDC identity provider HTTP/HTTPS read timeout.";
    private static final ConfigDef.Validator LOGIN_READ_TIMEOUT_MS_VALIDATOR = new OptionalNumberConfigDefValidator(0, false);

    private static final long LOGIN_RETRY_MAX_WAIT_MS_DEFAULT = 10000;
    private static final String LOGIN_RETRY_MAX_WAIT_MS_DOC = "The (optional) value in " +
        "milliseconds for the maximum wait between OAuth/OIDC identity provider HTTP/HTTPS " +
        "login attempts. " +
        "The login attempts use an exponential backoff algorithm with an initial " +
        "wait based on the " + LOGIN_RETRY_WAIT_MS_CONFIG + " setting and will double in wait " +
        "length between attempts up to a maximum wait length specified by " +
        LOGIN_RETRY_MAX_WAIT_MS_CONFIG + ".";

    private static final ConfigDef.Validator LOGIN_RETRY_MAX_WAIT_MS_VALIDATOR = new OptionalNumberConfigDefValidator(0);

    private static final long LOGIN_RETRY_WAIT_MS_DEFAULT = 250;
    private static final String LOGIN_RETRY_WAIT_MS_DOC = "The (optional) value in " +
        "milliseconds for the initial wait between OAuth/OIDC identity provider HTTP/HTTPS " +
        "login attempts. " +
        "The login attempts use an exponential backoff algorithm with an initial " +
        "wait based on the " + LOGIN_RETRY_WAIT_MS_CONFIG + " setting and will double in wait " +
        "length between attempts up to a maximum wait length specified by " +
        LOGIN_RETRY_MAX_WAIT_MS_CONFIG + ".";
    private static final ConfigDef.Validator LOGIN_RETRY_WAIT_MS_VALIDATOR = new OptionalNumberConfigDefValidator(0);

    private static final String SCOPE_DOC = "The (optional) HTTP/HTTPS login request to the " +
        "token endpoint (" + TOKEN_ENDPOINT_URI_CONFIG + ") may need to specify an OAuth " +
        "\"scope\". If so, the " + SCOPE_CONFIG + " " +
        "is used to provide the value to include with the login request.";

    private static final String SCOPE_CLAIM_NAME_DEFAULT = LoginAccessTokenValidator.SCOPE_CLAIM_NAME_DEFAULT;
    private static final String SCOPE_CLAIM_NAME_DOC = "The OAuth claim for the scope is often " +
        "named \"" + SCOPE_CLAIM_NAME_DEFAULT + "\", but this (optional) setting can provide " +
        "a different name to use for the scope included in the JWT payload's claims if the " +
        "OAuth/OIDC provider uses a different name for that claim.";

    private static final String SUB_CLAIM_NAME_DEFAULT = LoginAccessTokenValidator.SUBJECT_CLAIM_NAME_DEFAULT;
    private static final String SUB_CLAIM_NAME_DOC = "The OAuth claim for the subject is often " +
        "named \"" + SUB_CLAIM_NAME_DEFAULT + "\", but this (optional) setting can provide " +
        "a different name to use for the subject included in the JWT payload's claims if the " +
        "OAuth/OIDC provider uses a different name for that claim.";

    private static final String TOKEN_ENDPOINT_URI_DOC = "The OAuth/OIDC identity provider " +
        "HTTP/HTTPS issuer token endpoint URI to which requests will be made to login based " +
        "on the " + CLIENT_ID_CONFIG + ", " + CLIENT_SECRET_CONFIG + ", and optionally the " +
        SCOPE_CONFIG + ".";
    private static final ConfigDef.Validator TOKEN_ENDPOINT_URI_VALIDATOR = CompositeValidator.of(new NonEmptyString(), new UriConfigDefValidator());

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(ACCESS_TOKEN_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            ACCESS_TOKEN_DOC)
        .define(ACCESS_TOKEN_FILE_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            ACCESS_TOKEN_FILE_DOC)
        .define(CLIENT_ID_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            CLIENT_ID_DOC)
        .define(CLIENT_SECRET_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            CLIENT_SECRET_DOC)
        .define(LOGIN_ATTEMPTS_CONFIG,
            Type.INT,
            LOGIN_ATTEMPTS_DEFAULT,
            LOGIN_ATTEMPTS_VALIDATOR,
            Importance.MEDIUM,
            LOGIN_ATTEMPTS_DOC)
        .define(LOGIN_CONNECT_TIMEOUT_MS_CONFIG,
            Type.INT,
            null,
            LOGIN_CONNECT_TIMEOUT_MS_VALIDATOR,
            Importance.LOW,
            LOGIN_CONNECT_TIMEOUT_MS_DOC)
        .define(LOGIN_READ_TIMEOUT_MS_CONFIG,
            Type.INT,
            null,
            LOGIN_READ_TIMEOUT_MS_VALIDATOR,
            Importance.LOW,
            LOGIN_READ_TIMEOUT_MS_DOC)
        .define(LOGIN_RETRY_MAX_WAIT_MS_CONFIG,
            Type.LONG,
            LOGIN_RETRY_MAX_WAIT_MS_DEFAULT,
            LOGIN_RETRY_MAX_WAIT_MS_VALIDATOR,
            Importance.LOW,
            LOGIN_RETRY_MAX_WAIT_MS_DOC)
        .define(LOGIN_RETRY_WAIT_MS_CONFIG,
            Type.LONG,
            LOGIN_RETRY_WAIT_MS_DEFAULT,
            LOGIN_RETRY_WAIT_MS_VALIDATOR,
            Importance.LOW,
            LOGIN_RETRY_WAIT_MS_DOC)
        .define(SCOPE_CONFIG,
            Type.STRING,
            null,
            Importance.MEDIUM,
            SCOPE_DOC)
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
        .define(TOKEN_ENDPOINT_URI_CONFIG,
            Type.STRING,
            null,
            TOKEN_ENDPOINT_URI_VALIDATOR,
            Importance.MEDIUM,
            TOKEN_ENDPOINT_URI_DOC);

    public LoginCallbackHandlerConfiguration(Map<String, ?> options) {
        super(CONFIG, options);
    }

    public String getAccessToken() {
        return getString(ACCESS_TOKEN_CONFIG);
    }

    public String getAccessTokenFile() {
        return getString(ACCESS_TOKEN_FILE_CONFIG);
    }

    public String getClientId() {
        return getString(CLIENT_ID_CONFIG);
    }

    public String getClientSecret() {
        return getString(CLIENT_SECRET_CONFIG);
    }

    public int getLoginAttempts() {
        return getInt(LOGIN_ATTEMPTS_CONFIG);
    }

    public Integer getLoginConnectTimeoutMs() {
        return getInt(LOGIN_CONNECT_TIMEOUT_MS_CONFIG);
    }

    public Integer getLoginReadTimeoutMs() {
        return getInt(LOGIN_READ_TIMEOUT_MS_CONFIG);
    }

    public long getLoginRetryMaxWaitMs() {
        return getLong(LOGIN_RETRY_MAX_WAIT_MS_CONFIG);
    }

    public long getLoginRetryWaitMs() {
        return getLong(LOGIN_RETRY_WAIT_MS_CONFIG);
    }

    public String getScope() {
        return getString(SCOPE_CONFIG);
    }

    public String getScopeClaimName() {
        return getString(SCOPE_CLAIM_NAME_CONFIG);
    }

    public String getSubClaimName() {
        return getString(SUB_CLAIM_NAME_CONFIG);
    }

    public String getTokenEndpointUri() {
        return getString(TOKEN_ENDPOINT_URI_CONFIG);
    }

}
