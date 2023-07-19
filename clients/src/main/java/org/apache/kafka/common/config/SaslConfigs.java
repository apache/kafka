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
package org.apache.kafka.common.config;

import org.apache.kafka.common.config.ConfigDef.Range;

public class SaslConfigs {

    private static final String OAUTHBEARER_NOTE = " Currently applies only to OAUTHBEARER.";

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */
    /** SASL mechanism configuration - standard mechanism names are listed <a href="http://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml">here</a>. */
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SASL_MECHANISM_DOC = "SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism.";
    public static final String GSSAPI_MECHANISM = "GSSAPI";
    public static final String DEFAULT_SASL_MECHANISM = GSSAPI_MECHANISM;

    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_JAAS_CONFIG_DOC = "JAAS login context parameters for SASL connections in the format used by JAAS configuration files. "
        + "JAAS configuration file format is described <a href=\"https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html\">here</a>. "
        + "The format for the value is: <code>loginModuleClass controlFlag (optionName=optionValue)*;</code>. For brokers, "
        + "the config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example, "
        + "listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=com.example.ScramLoginModule required;";

    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class";
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL client callback handler class "
        + "that implements the AuthenticateCallbackHandler interface.";

    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL login callback handler class "
            + "that implements the AuthenticateCallbackHandler interface. For brokers, login callback handler config must be prefixed with "
            + "listener prefix and SASL mechanism name in lower-case. For example, "
            + "listener.name.sasl_ssl.scram-sha-256.sasl.login.callback.handler.class=com.example.CustomScramLoginCallbackHandler";

    public static final String SASL_LOGIN_CLASS = "sasl.login.class";
    public static final String SASL_LOGIN_CLASS_DOC = "The fully qualified name of a class that implements the Login interface. "
        + "For brokers, login config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example, "
        + "listener.name.sasl_ssl.scram-sha-256.sasl.login.class=com.example.CustomScramLogin";

    public static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    public static final String SASL_KERBEROS_SERVICE_NAME_DOC = "The Kerberos principal name that Kafka runs as. "
        + "This can be defined either in Kafka's JAAS config or in Kafka's config.";

    public static final String SASL_KERBEROS_KINIT_CMD = "sasl.kerberos.kinit.cmd";
    public static final String SASL_KERBEROS_KINIT_CMD_DOC = "Kerberos kinit command path.";
    public static final String DEFAULT_KERBEROS_KINIT_CMD = "/usr/bin/kinit";

    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = "sasl.kerberos.ticket.renew.window.factor";
    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC = "Login thread will sleep until the specified window factor of time from last refresh"
        + " to ticket's expiry has been reached, at which time it will try to renew the ticket.";
    public static final double DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = 0.80;

    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER = "sasl.kerberos.ticket.renew.jitter";
    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER_DOC = "Percentage of random jitter added to the renewal time.";
    public static final double DEFAULT_KERBEROS_TICKET_RENEW_JITTER = 0.05;

    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN = "sasl.kerberos.min.time.before.relogin";
    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC = "Login thread sleep time between refresh attempts.";
    public static final long DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;

    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR = "sasl.login.refresh.window.factor";
    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC = "Login refresh thread will sleep until the specified window factor relative to the"
            + " credential's lifetime has been reached, at which time it will try to refresh the credential."
            + " Legal values are between 0.5 (50%) and 1.0 (100%) inclusive; a default value of 0.8 (80%) is used"
            + " if no value is specified."
            + OAUTHBEARER_NOTE;
    public static final double DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR = 0.80;

    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER = "sasl.login.refresh.window.jitter";
    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC = "The maximum amount of random jitter relative to the credential's lifetime"
            + " that is added to the login refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) inclusive;"
            + " a default value of 0.05 (5%) is used if no value is specified."
            + OAUTHBEARER_NOTE;
    public static final double DEFAULT_LOGIN_REFRESH_WINDOW_JITTER = 0.05;

    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS = "sasl.login.refresh.min.period.seconds";
    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC = "The desired minimum time for the login refresh thread to wait before refreshing a credential,"
            + " in seconds. Legal values are between 0 and 900 (15 minutes); a default value of 60 (1 minute) is used if no value is specified.  This value and "
            + " sasl.login.refresh.buffer.seconds are both ignored if their sum exceeds the remaining lifetime of a credential."
            + OAUTHBEARER_NOTE;
    public static final short DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS = 60;

    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS = "sasl.login.refresh.buffer.seconds";
    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC = "The amount of buffer time before credential expiration to maintain when refreshing a credential,"
            + " in seconds. If a refresh would otherwise occur closer to expiration than the number of buffer seconds then the refresh will be moved up to maintain"
            + " as much of the buffer time as possible. Legal values are between 0 and 3600 (1 hour); a default value of  300 (5 minutes) is used if no value is specified."
            + " This value and sasl.login.refresh.min.period.seconds are both ignored if their sum exceeds the remaining lifetime of a credential."
            + OAUTHBEARER_NOTE;
    public static final short DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS = 300;

    public static final String SASL_LOGIN_CONNECT_TIMEOUT_MS = "sasl.login.connect.timeout.ms";
    public static final String SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC = "The (optional) value in milliseconds for the external authentication provider connection timeout."
            + OAUTHBEARER_NOTE;

    public static final String SASL_LOGIN_READ_TIMEOUT_MS = "sasl.login.read.timeout.ms";
    public static final String SASL_LOGIN_READ_TIMEOUT_MS_DOC = "The (optional) value in milliseconds for the external authentication provider read timeout."
            + OAUTHBEARER_NOTE;

    private static final String LOGIN_EXPONENTIAL_BACKOFF_NOTE = " Login uses an exponential backoff algorithm with an initial wait based on the"
            + " sasl.login.retry.backoff.ms setting and will double in wait length between attempts up to a maximum wait length specified by the"
            + " sasl.login.retry.backoff.max.ms setting."
            + OAUTHBEARER_NOTE;

    public static final String SASL_LOGIN_RETRY_BACKOFF_MAX_MS = "sasl.login.retry.backoff.max.ms";
    public static final long DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS = 10000;
    public static final String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC = "The (optional) value in milliseconds for the maximum wait between login attempts to the"
            + " external authentication provider."
            + LOGIN_EXPONENTIAL_BACKOFF_NOTE;

    public static final String SASL_LOGIN_RETRY_BACKOFF_MS = "sasl.login.retry.backoff.ms";
    public static final long DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS = 100;
    public static final String SASL_LOGIN_RETRY_BACKOFF_MS_DOC = "The (optional) value in milliseconds for the initial wait between login attempts to the external"
            + " authentication provider."
            + LOGIN_EXPONENTIAL_BACKOFF_NOTE;

    public static final String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME = "sasl.oauthbearer.scope.claim.name";
    public static final String DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME = "scope";
    public static final String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC = "The OAuth claim for the scope is often named \"" + DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME + "\", but this (optional)"
            + " setting can provide a different name to use for the scope included in the JWT payload's claims if the OAuth/OIDC provider uses a different"
            + " name for that claim.";

    public static final String SASL_OAUTHBEARER_SUB_CLAIM_NAME = "sasl.oauthbearer.sub.claim.name";
    public static final String DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME = "sub";
    public static final String SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC = "The OAuth claim for the subject is often named \"" + DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME + "\", but this (optional)"
            + " setting can provide a different name to use for the subject included in the JWT payload's claims if the OAuth/OIDC provider uses a different"
            + " name for that claim.";

    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC = "The URL for the OAuth/OIDC identity provider. If the URL is HTTP(S)-based, it is the issuer's token"
            + " endpoint URL to which requests will be made to login based on the configuration in " + SASL_JAAS_CONFIG + ". If the URL is file-based, it"
            + " specifies a file containing an access token (in JWT serialized form) issued by the OAuth/OIDC identity provider to use for authorization.";

    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url";
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC = "The OAuth/OIDC provider URL from which the provider's"
            + " <a href=\"https://datatracker.ietf.org/doc/html/rfc7517#section-5\">JWKS (JSON Web Key Set)</a> can be retrieved. The URL can be HTTP(S)-based or file-based."
            + " If the URL is HTTP(S)-based, the JWKS data will be retrieved from the OAuth/OIDC provider via the configured URL on broker startup. All then-current"
            + " keys will be cached on the broker for incoming requests. If an authentication request is received for a JWT that includes a \"kid\" header claim value that"
            + " isn't yet in the cache, the JWKS endpoint will be queried again on demand. However, the broker polls the URL every sasl.oauthbearer.jwks.endpoint.refresh.ms"
            + " milliseconds to refresh the cache with any forthcoming keys before any JWT requests that include them are received."
            + " If the URL is file-based, the broker will load the JWKS file from a configured location on startup. In the event that the JWT includes a \"kid\" header"
            + " value that isn't in the JWKS file, the broker will reject the JWT and authentication will fail.";

    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS = "sasl.oauthbearer.jwks.endpoint.refresh.ms";
    public static final long DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS = 60 * 60 * 1000;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC = "The (optional) value in milliseconds for the broker to wait between refreshing its JWKS (JSON Web Key Set)"
            + " cache that contains the keys to verify the signature of the JWT.";

    private static final String JWKS_EXPONENTIAL_BACKOFF_NOTE = " JWKS retrieval uses an exponential backoff algorithm with an initial wait based on the"
        + " sasl.oauthbearer.jwks.endpoint.retry.backoff.ms setting and will double in wait length between attempts up to a maximum wait length specified by the"
        + " sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms setting.";

    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS = "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms";
    public static final long DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS = 10000;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC = "The (optional) value in milliseconds for the maximum wait between attempts to retrieve the JWKS (JSON Web Key Set)"
        + " from the external authentication provider."
        + JWKS_EXPONENTIAL_BACKOFF_NOTE;

    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS = "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms";
    public static final long DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS = 100;
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC = "The (optional) value in milliseconds for the initial wait between JWKS (JSON Web Key Set) retrieval attempts from the external"
        + " authentication provider."
        + JWKS_EXPONENTIAL_BACKOFF_NOTE;

    public static final String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS = "sasl.oauthbearer.clock.skew.seconds";
    public static final int DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS = 30;
    public static final String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC = "The (optional) value in seconds to allow for differences between the time of the OAuth/OIDC identity provider and"
            + " the broker.";

    public static final String SASL_OAUTHBEARER_EXPECTED_AUDIENCE = "sasl.oauthbearer.expected.audience";
    public static final String SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC = "The (optional) comma-delimited setting for the broker to use to verify that the JWT was issued for one of the"
            + " expected audiences. The JWT will be inspected for the standard OAuth \"aud\" claim and if this value is set, the broker will match the value from JWT's \"aud\" claim "
            + " to see if there is an exact match. If there is no match, the broker will reject the JWT and authentication will fail.";

    public static final String SASL_OAUTHBEARER_EXPECTED_ISSUER = "sasl.oauthbearer.expected.issuer";
    public static final String SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC = "The (optional) setting for the broker to use to verify that the JWT was created by the expected issuer. The JWT will"
            + " be inspected for the standard OAuth \"iss\" claim and if this value is set, the broker will match it exactly against what is in the JWT's \"iss\" claim. If there is no"
            + " match, the broker will reject the JWT and authentication will fail.";

    public static void addClientSaslSupport(ConfigDef config) {
        config.define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
                .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
                .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Type.LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, Range.between(0.5, 1.0), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER, Range.between(0.0, 0.25), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, ConfigDef.Type.SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, Range.between(0, 900), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, ConfigDef.Type.SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, Range.between(0, 3600), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
                .define(SaslConfigs.SASL_MECHANISM, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_MECHANISM_DOC)
                .define(SaslConfigs.SASL_JAAS_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_JAAS_CONFIG_DOC)
                .define(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_LOGIN_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS, ConfigDef.Type.INT, null, ConfigDef.Importance.LOW, SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
                .define(SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS, ConfigDef.Type.INT, null, ConfigDef.Importance.LOW, SASL_LOGIN_READ_TIMEOUT_MS_DOC)
                .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS, ConfigDef.Type.LONG, DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS, ConfigDef.Importance.LOW, SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
                .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS, ConfigDef.Type.LONG, DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS, ConfigDef.Importance.LOW, SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, ConfigDef.Type.STRING, DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, ConfigDef.Type.STRING, DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, ConfigDef.Type.LONG, DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, ConfigDef.Type.LONG, DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, ConfigDef.Type.LONG, DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, ConfigDef.Type.INT, DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC);
    }
}
