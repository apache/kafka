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

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;

public class LoginCallbackHandlerConfiguration extends AbstractConfig {

    public static final String CLIENT_ID_CONFIG = "clientId";
    private static final String CLIENT_ID_DOC = "xxx";
    private static final ConfigDef.Validator CLIENT_ID_VALIDATOR = new NonEmptyString();

    public static final String CLIENT_SECRET_CONFIG = "clientSecret";
    private static final String CLIENT_SECRET_DOC = "xxx";
    private static final ConfigDef.Validator CLIENT_SECRET_VALIDATOR = new NonEmptyString();

    public static final String LOGIN_ATTEMPTS_CONFIG = "loginAttempts";
    private static final int LOGIN_ATTEMPTS_DEFAULT = 3;
    private static final String LOGIN_ATTEMPTS_DOC = "xxx";
    private static final ConfigDef.Validator LOGIN_ATTEMPTS_VALIDATOR = Range.atLeast(1);

    public static final String LOGIN_CONNECT_TIMEOUT_MS_CONFIG = "loginConnectTimeoutMs";
    private static final long LOGIN_CONNECT_TIMEOUT_MS_DEFAULT = 10000;
    private static final String LOGIN_CONNECT_TIMEOUT_MS_DOC = "xxx";
    private static final ConfigDef.Validator LOGIN_CONNECT_TIMEOUT_MS_VALIDATOR = Range.atLeast(0);

    public static final String LOGIN_READ_TIMEOUT_MS_CONFIG = "loginReadTimeoutMs";
    private static final long LOGIN_READ_TIMEOUT_MS_DEFAULT = 10000;
    private static final String LOGIN_READ_TIMEOUT_MS_DOC = "xxx";
    private static final ConfigDef.Validator LOGIN_READ_TIMEOUT_MS_VALIDATOR = Range.atLeast(0);

    public static final String LOGIN_RETRY_MAX_WAIT_MS_CONFIG = "loginRetryMaxWaitMs";
    private static final long LOGIN_RETRY_MAX_WAIT_MS_DEFAULT = 10000;
    private static final String LOGIN_RETRY_MAX_WAIT_MS_DOC = "xxx";
    private static final ConfigDef.Validator LOGIN_RETRY_MAX_WAIT_MS_VALIDATOR = Range.atLeast(0);

    public static final String LOGIN_RETRY_WAIT_MS_CONFIG = "loginRetryWaitMs";
    private static final long LOGIN_RETRY_WAIT_MS_DEFAULT = 250;
    private static final String LOGIN_RETRY_WAIT_MS_DOC = "xxx";
    private static final ConfigDef.Validator LOGIN_RETRY_WAIT_MS_VALIDATOR = Range.atLeast(0);

    public static final String SCOPE_CONFIG = "scope";
    private static final String SCOPE_DOC = "xxx";

    public static final String SCOPE_CLAIM_NAME_CONFIG = "scopeClaimName";
    private static final String SCOPE_CLAIM_NAME_DEFAULT = "scope";
    private static final String SCOPE_CLAIM_NAME_DOC = "xxx";

    public static final String SUB_CLAIM_NAME_CONFIG = "subClaimName";
    private static final String SUB_CLAIM_NAME_DEFAULT = "sub";
    private static final String SUB_CLAIM_NAME_DOC = "xxx";

    public static final String TOKEN_ENDPOINT_URI_CONFIG = "tokenEndpointUri";
    private static final String TOKEN_ENDPOINT_URI_DOC = "xxx";
    private static final ConfigDef.Validator TOKEN_ENDPOINT_URI_VALIDATOR = new UriConfigDefValidator(true);

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(CLIENT_ID_CONFIG,
            Type.STRING,
            NO_DEFAULT_VALUE,
            CLIENT_ID_VALIDATOR,
            Importance.HIGH,
            CLIENT_ID_DOC)
        .define(CLIENT_SECRET_CONFIG,
            Type.STRING,
            NO_DEFAULT_VALUE,
            CLIENT_SECRET_VALIDATOR,
            Importance.HIGH,
            CLIENT_SECRET_DOC)
        .define(LOGIN_ATTEMPTS_CONFIG,
            Type.INT,
            LOGIN_ATTEMPTS_DEFAULT,
            LOGIN_ATTEMPTS_VALIDATOR,
            Importance.MEDIUM,
            LOGIN_ATTEMPTS_DOC)
        .define(LOGIN_CONNECT_TIMEOUT_MS_CONFIG,
            Type.LONG,
            LOGIN_CONNECT_TIMEOUT_MS_DEFAULT,
            LOGIN_CONNECT_TIMEOUT_MS_VALIDATOR,
            Importance.LOW,
            LOGIN_CONNECT_TIMEOUT_MS_DOC)
        .define(LOGIN_READ_TIMEOUT_MS_CONFIG,
            Type.LONG,
            LOGIN_READ_TIMEOUT_MS_DEFAULT,
            LOGIN_READ_TIMEOUT_MS_VALIDATOR,
            Importance.LOW,
            LOGIN_READ_TIMEOUT_MS_DOC)
        .define(LOGIN_RETRY_MAX_WAIT_MS_DOC,
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
            NO_DEFAULT_VALUE,
            TOKEN_ENDPOINT_URI_VALIDATOR,
            Importance.MEDIUM,
            TOKEN_ENDPOINT_URI_DOC)
        ;

    public LoginCallbackHandlerConfiguration(Map<String, ?> options) {
        super(CONFIG, options);
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

    public long getLoginConnectTimeoutMs() {
        return getLong(LOGIN_CONNECT_TIMEOUT_MS_CONFIG);
    }

    public long getLoginReadTimeoutMs() {
        return getLong(LOGIN_READ_TIMEOUT_MS_CONFIG);
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
