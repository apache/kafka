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

import org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString;
import org.apache.kafka.common.config.ConfigDef.Range;

import java.util.List;

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

    public static final String SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS = "sasl.oauthbearer.jwt.retriever.class";
    public static final String DEFAULT_SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS = "org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever";
    public static final String SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS_DOC = "<p>The fully-qualified class name of a <code>JwtRetriever</code> implementation used to"
        + " request tokens from the identity provider.</p>"
        + "<p>The default configuration value represents a class that maintains backward compatibility with previous versions of"
        + " Apache Kafka. The default implementation uses the configuration to determine which concrete implementation to create."
        + "<p>Other implementations that are provided include:</p>"
        + "<ul>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever</code></li>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever</code></li>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.FileJwtRetriever</code></li>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever</code></li>"
        + "</ul>";

    public static final String SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS = "sasl.oauthbearer.jwt.validator.class";
    public static final String DEFAULT_SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS = "org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator";
    public static final String SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS_DOC = "<p>The fully-qualified class name of a <code>JwtValidator</code> implementation used to"
        + " validate the JWT from the identity provider.</p>"
        + "<p>The default validator (<code>org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator</code>) maintains backward compatibility with previous"
        + " versions of Apache Kafka. The default validator uses configuration to determine which concrete implementation to create."
        + "<p>The built-in <code>JwtValidator</code> implementations are:</p>"
        + "<ul>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator</code></li>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.ClientJwtValidator</code></li>"
        + "<li><code>org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator</code></li>"
        + "</ul>";

    public static final String SASL_OAUTHBEARER_SCOPE = "sasl.oauthbearer.scope";
    public static final String SASL_OAUTHBEARER_SCOPE_DOC = "<p>This is the level of access a client application is granted to a resource or API which is"
        + " included in the token request. If provided, it should match one or more scopes configured in the identity provider.</p>"
        + "<p>"
        + "The scope was previously stored as part of the <code>sasl.jaas.config</code> configuration with the key <code>scope</code>."
        + " For backward compatibility, the <code>scope</code> JAAS option can still be used, but it is deprecated and will be removed in a future version."
        + "</p>"
        + "<p>Order of precedence:</p>"
        + "<ul>"
        + "<li><code>sasl.oauthbearer.scope</code> from configuration</li>"
        + "<li><code>scope</code> from JAAS</li>"
        + "</ul>";

    public static final String SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID = "sasl.oauthbearer.client.credentials.client.id";
    public static final String SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC = "<p>The ID (defined in/by the OAuth identity provider) to identify the client" +
        " requesting the token.</p>"
        + "<p>"
        + "The client ID was previously stored as part of the <code>sasl.jaas.config</code> configuration with the key <code>clientId</code>."
        + " For backward compatibility, the <code>clientId</code> JAAS option can still be used, but it is deprecated and will be removed in a future version."
        + "</p>"
        + "<p>Order of precedence:</p>"
        + "<ul>"
        + "<li><code>sasl.oauthbearer.client.credentials.client.id</code> from configuration</li>"
        + "<li><code>clientId</code> from JAAS</li>"
        + "</ul>";

    public static final String SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET = "sasl.oauthbearer.client.credentials.client.secret";
    public static final String SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC = "<p>The secret (defined by either the user or preassigned, depending on the"
        + " identity provider) of the client requesting the token.</p>"
        + "<p>"
        + "The client secret was previously stored as part of the <code>sasl.jaas.config</code> configuration with the key <code>clientSecret</code>."
        + " For backward compatibility, the <code>clientSecret</code> JAAS option can still be used, but it is deprecated and will be removed in a future version."
        + "</p>"
        + "<p>Order of precedence:</p>"
        + "<ul>"
        + "<li><code>sasl.oauthbearer.client.credentials.client.secret</code> from configuration</li>"
        + "<li><code>clientSecret</code> from JAAS</li>"
        + "</ul>";

    private static final String ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE = "<p><em>Note</em>: If a value for <code>sasl.oauthbearer.assertion.file</code> is provided,"
        + " this configuration will be ignored.</p>";

    public static final String SASL_OAUTHBEARER_ASSERTION_ALGORITHM = "sasl.oauthbearer.assertion.algorithm";
    public static final String DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM = "RS256";
    public static final String SASL_OAUTHBEARER_ASSERTION_ALGORITHM_DOC = "<p>The algorithm the Apache Kafka client should use to sign the assertion sent"
        + " to the identity provider. It is also used as the value of the OAuth <code>alg</code> (Algorithm) header in the JWT assertion.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD = "sasl.oauthbearer.assertion.claim.aud";
    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD_DOC = "<p>The JWT <code>aud</code> (Audience) claim which will be included in the "
        + " client JWT assertion created locally.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS = "sasl.oauthbearer.assertion.claim.exp.seconds";
    public static final int DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS = 300;
    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS_DOC = "<p>The number of seconds <em>in the future</em> for which the JWT is valid."
        + " The value is used to determine the JWT <code>exp</code> (Expiration) claim based on the current system time when the JWT is created.</p>"
        + "<p>The formula to generate the <code>exp</code> claim is very simple:</p>"
        + "<pre>"
        + "Let:\n\n"
        + "  x = the current timestamp in seconds, on client\n"
        + "  y = the value of this configuration\n"
        + "\n"
        + "Then:\n\n"
        + "  exp = x + y\n"
        + "</pre>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS = "sasl.oauthbearer.assertion.claim.iss";
    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS_DOC = "<p>The value to be used as the <code>iss</code> (Issuer) claim which will be included in the"
        + " client JWT assertion created locally.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE = "sasl.oauthbearer.assertion.claim.jti.include";
    public static final boolean DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE = false;
    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE_DOC = "<p>Flag that determines if the JWT assertion should generate a unique ID for the"
        + " JWT and include it in the <code>jti</code> (JWT ID) claim.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS = "sasl.oauthbearer.assertion.claim.nbf.seconds";
    public static final int DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS = 60;
    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS_DOC = "<p>The number of seconds <em>in the past</em> from which the JWT is valid."
        + " The value is used to determine the JWT <code>nbf</code> (Not Before) claim based on the current system time when the JWT is created.</p>"
        + "<p>The formula to generate the <code>nbf</code> claim is very simple:</p>"
        + "<pre>"
        + "Let:\n\n"
        + "  x = the current timestamp in seconds, on client\n"
        + "  y = the value of this configuration\n"
        + "\n"
        + "Then:\n\n"
        + "  nbf = x - y\n"
        + "</pre>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB = "sasl.oauthbearer.assertion.claim.sub";
    public static final String SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB_DOC = "<p>The value to be used as the <code>sub</code> (Subject) claim which will be included in the"
        + " client JWT assertion created locally.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_FILE = "sasl.oauthbearer.assertion.file";
    public static final String SASL_OAUTHBEARER_ASSERTION_FILE_DOC = "<p>File that contains a <em>pre-generated</em> JWT assertion.</p>"
        + "<p>The underlying implementation caches the file contents to avoid the performance hit of loading the file on each access. The caching mechanism will detect when"
        + "the file changes to allow for the file to be reloaded on modifications. This allows for &quot;live&quot; assertion rotation without restarting the Kafka client.</p>"
        + "<p>The file contains the assertion in the serialized, three part JWT format:</p>"
        + "<ol>"
        + "<li>The <em>header</em> section is a base 64-encoded JWT header that contains values like <code>alg</code> (Algorithm),"
        + " <code>typ</code> (Type, always the literal value <code>JWT</code>), etc.</li>"
        + "<li>The <em>payload</em> section includes the base 64-encoded set of JWT claims, such as <code>aud</code> (Audience), <code>iss</code> (Issuer),"
        + " <code>sub</code> (Subject), etc.</li>"
        + "<li>The <em>signature</em> section is the concatenated <em>header</em> and <em>payload</em> sections that was signed using a private key</li>"
        + "</ol>"
        + "<p>See <a href=\"https://datatracker.ietf.org/doc/html/rfc7519\">RFC 7519</a> and <a href=\"https://datatracker.ietf.org/doc/html/rfc7515\">RFC 7515</a>"
        + " for more details on the JWT and JWS formats.</p>"
        + "<p><em>Note</em>: If a value for <code>sasl.oauthbearer.assertion.file</code> is provided, all other"
        + " <code>sasl.oauthbearer.assertion.</code>* configurations are ignored.</p>";

    public static final String SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE = "sasl.oauthbearer.assertion.private.key.file";
    public static final String SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE_DOC =  "<p>File that contains a private key in the standard PEM format which is used to"
        + " sign the JWT assertion sent to the identity provider.</p>"
        + "<p>The underlying implementation caches the file contents to avoid the performance hit of loading the file on each access. The caching mechanism will detect when"
        + " the file changes to allow for the file to be reloaded on modifications. This allows for &quot;live&quot; private key rotation without restarting the Kafka client.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE = "sasl.oauthbearer.assertion.private.key.passphrase";
    public static final String SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE_DOC = "<p>The optional passphrase to decrypt the private key file specified by"
        + " <code>sasl.oauthbearer.assertion.private.key.file</code>.</p>"
        + "<p><em>Note</em>: If the file referred to by <code>sasl.oauthbearer.assertion.private.key.file</code> is modified on the file system at runtime and it was"
        + " created with a <em>different</em> passphrase than it was previously, the client will not be able to access the private key file because the passphrase is now"
        + " out of date. For that reason, when using private key passphrases, either use the same passphrase each time, or&mdash;for improved security&mdash;restart"
        + " the Kafka client using the new passphrase configuration.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

    public static final String SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE = "sasl.oauthbearer.assertion.template.file";
    public static final String SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE_DOC = "<p>This optional configuration specifies the file containing the JWT headers and/or"
        + " payload claims to be used when creating the JWT assertion.</p>"
        + "<p>Not all identity providers require the same set of claims; some may require a given claim while others may prohibit it."
        + " In order to provide the most flexibility, this configuration allows the user to provide the static header values and claims"
        + " that are to be included in the JWT.</p>"
        + ASSERTION_FILE_MUTUAL_EXCLUSION_NOTICE;

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
            + " endpoint URL to which requests will be made to login based on the configuration in <code>" + SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS + "</code>. If the URL is"
            + " file-based, it specifies a file containing an access token (in JWT serialized form) issued by the OAuth/OIDC identity provider to use for authorization.";

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

    public static final String SASL_OAUTHBEARER_HEADER_URLENCODE = "sasl.oauthbearer.header.urlencode";
    public static final boolean DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE = false;
    public static final String SASL_OAUTHBEARER_HEADER_URLENCODE_DOC = "The (optional) setting to enable the OAuth client to URL-encode the client_id and client_secret in the authorization header"
            + " in accordance with RFC6749, see <a href=\"https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1\">here</a> for more details. The default value is set to 'false' for backward compatibility";
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
                .define(SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, ConfigDef.Type.CLASS, DEFAULT_SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS, ConfigDef.Type.CLASS, DEFAULT_SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_SCOPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_SCOPE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM, ConfigDef.Type.STRING, DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM, CaseInsensitiveValidString.in("ES256", "RS256"), ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_ALGORITHM_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, ConfigDef.Type.INT, DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, Range.between(0, 86400), ConfigDef.Importance.LOW, SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, ConfigDef.Type.BOOLEAN, DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, ConfigDef.Type.INT, DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, Range.between(0, 3600), ConfigDef.Importance.LOW, SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_FILE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, ConfigDef.Type.STRING, DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, ConfigDef.Type.STRING, DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, ConfigDef.Type.LONG, DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, ConfigDef.Type.LONG, DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, ConfigDef.Type.LONG, DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, ConfigDef.Type.INT, DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, ConfigDef.Type.LIST, List.of(), ConfigDef.ValidList.anyNonDuplicateValues(true, false), ConfigDef.Importance.LOW, SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC)
                .define(SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE, ConfigDef.Type.BOOLEAN, DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE, ConfigDef.Importance.LOW, SASL_OAUTHBEARER_HEADER_URLENCODE_DOC);
    }
}
