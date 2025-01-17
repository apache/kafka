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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * Common home for broker-side security configs which need to be accessible from the libraries shared
 * between the broker and the client.
 *
 * Note this is an internal API and subject to change without notice.
 */
public class BrokerSecurityConfigs {

    public static final String PRINCIPAL_BUILDER_CLASS_CONFIG = "principal.builder.class";

    public static final String SSL_PRINCIPAL_MAPPING_RULES_CONFIG = "ssl.principal.mapping.rules";
    public static final String DEFAULT_SSL_PRINCIPAL_MAPPING_RULES = "DEFAULT";
    public static final String SSL_PRINCIPAL_MAPPING_RULES_DOC = "A list of rules for mapping from distinguished name" +
            " from the client certificate to short name. The rules are evaluated in order and the first rule that matches" +
            " a principal name is used to map it to a short name. Any later rules in the list are ignored. By default," +
            " distinguished name of the X.500 certificate will be the principal. For more details on the format please" +
            " see <a href=\"#security_authz\"> security authorization and acls</a>. Note that this configuration is ignored" +
            " if an extension of KafkaPrincipalBuilder is provided by the <code>" + PRINCIPAL_BUILDER_CLASS_CONFIG + "</code>" +
            " configuration.";

    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG = "sasl.kerberos.principal.to.local.rules";
    public static final List<String> DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = Collections.singletonList(DEFAULT_SSL_PRINCIPAL_MAPPING_RULES);
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = "A list of rules for mapping from principal " +
            "names to short names (typically operating system usernames). The rules are evaluated in order and the " +
            "first rule that matches a principal name is used to map it to a short name. Any later rules in the list are " +
            "ignored. By default, principal names of the form <code>{username}/{hostname}@{REALM}</code> are mapped " +
            "to <code>{username}</code>. For more details on the format please see <a href=\"#security_authz\"> " +
            "security authorization and acls</a>. Note that this configuration is ignored if an extension of " +
            "<code>KafkaPrincipalBuilder</code> is provided by the <code>" + PRINCIPAL_BUILDER_CLASS_CONFIG + "</code> configuration.";

    public static final Class<? extends KafkaPrincipalBuilder> PRINCIPAL_BUILDER_CLASS_DEFAULT = DefaultKafkaPrincipalBuilder.class;
    public static final String PRINCIPAL_BUILDER_CLASS_DOC = "The fully qualified name of a class that implements the " +
            "KafkaPrincipalBuilder interface, which is used to build the KafkaPrincipal object used during " +
            "authorization. If no principal builder is defined, the default behavior depends " +
            "on the security protocol in use. For SSL authentication,  the principal will be derived using the " +
            "rules defined by <code>" + SSL_PRINCIPAL_MAPPING_RULES_CONFIG + "</code> applied on the distinguished " +
            "name from the client certificate if one is provided; otherwise, if client authentication is not required, " +
            "the principal name will be ANONYMOUS. For SASL authentication, the principal will be derived using the " +
            "rules defined by <code>" + SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG + "</code> if GSSAPI is in use, " +
            "and the SASL authentication ID for other mechanisms. For PLAINTEXT, the principal will be ANONYMOUS.";

    public static final String SSL_CLIENT_AUTH_CONFIG = "ssl.client.auth";
    public static final String SSL_CLIENT_AUTH_DEFAULT = SslClientAuth.NONE.toString();
    public static final String SSL_CLIENT_AUTH_DOC = "Configures kafka broker to request client authentication."
            + " The following settings are common: "
            + " <ul>"
            + " <li><code>ssl.client.auth=required</code> If set to required client authentication is required."
            + " <li><code>ssl.client.auth=requested</code> This means client authentication is optional."
            + " unlike required, if this option is set client can choose not to provide authentication information about itself"
            + " <li><code>ssl.client.auth=none</code> This means client authentication is not needed."
            + "</ul>";

    public static final String SASL_ENABLED_MECHANISMS_CONFIG = "sasl.enabled.mechanisms";
    public static final List<String> DEFAULT_SASL_ENABLED_MECHANISMS = Collections.singletonList(SaslConfigs.GSSAPI_MECHANISM);
    public static final String SASL_ENABLED_MECHANISMS_DOC = "The list of SASL mechanisms enabled in the Kafka server. "
            + "The list may contain any mechanism for which a security provider is available. "
            + "Only GSSAPI is enabled by default.";

    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG = "sasl.server.callback.handler.class";
    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL server callback handler "
            + "class that implements the AuthenticateCallbackHandler interface. Server callback handlers must be prefixed with "
            + "listener prefix and SASL mechanism name in lower-case. For example, "
            + "listener.name.sasl_ssl.plain.sasl.server.callback.handler.class=com.example.CustomPlainCallbackHandler.";

    public static final String CONNECTIONS_MAX_REAUTH_MS_CONFIG = "connections.max.reauth.ms";
    public static final long DEFAULT_CONNECTIONS_MAX_REAUTH_MS = 0L;
    public static final String CONNECTIONS_MAX_REAUTH_MS_DOC = "When explicitly set to a positive number (the default is 0, not a positive number), "
            + "a session lifetime that will not exceed the configured value will be communicated to v2.2.0 or later clients when they authenticate. "
            + "The broker will disconnect any such connection that is not re-authenticated within the session lifetime and that is then subsequently "
            + "used for any purpose other than re-authentication. Configuration names can optionally be prefixed with listener prefix and SASL "
            + "mechanism name in lower-case. For example, listener.name.sasl_ssl.oauthbearer.connections.max.reauth.ms=3600000";

    public static final String SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG = "sasl.server.max.receive.size";
    public static final int DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE = 524288;
    public static final String SASL_SERVER_MAX_RECEIVE_SIZE_DOC = "The maximum receive size allowed before and during initial SASL authentication." +
            " Default receive size is 512KB. GSSAPI limits requests to 64K, but we allow upto 512KB by default for custom SASL mechanisms. In practice," +
            " PLAIN, SCRAM and OAUTH mechanisms can use much smaller limits.";

    public static final String SSL_ALLOW_DN_CHANGES_CONFIG = "ssl.allow.dn.changes";
    public static final boolean DEFAULT_SSL_ALLOW_DN_CHANGES_VALUE = false;
    public static final String SSL_ALLOW_DN_CHANGES_DOC = "Indicates whether changes to the certificate distinguished name should be allowed during" +
            " a dynamic reconfiguration of certificates or not.";

    public static final String SSL_ALLOW_SAN_CHANGES_CONFIG = "ssl.allow.san.changes";
    public static final boolean DEFAULT_SSL_ALLOW_SAN_CHANGES_VALUE = false;
    public static final String SSL_ALLOW_SAN_CHANGES_DOC = "Indicates whether changes to the certificate subject alternative names should be allowed during " +
            "a dynamic reconfiguration of certificates or not.";

    public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG = "sasl.mechanism.inter.broker.protocol";
    public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC = "SASL mechanism used for inter-broker communication. Default is GSSAPI.";

    // The allowlist of the SASL OAUTHBEARER endpoints
    public static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";
    public static final String ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT = "";
    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            // General Security Configuration
            .define(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, LONG, BrokerSecurityConfigs.DEFAULT_CONNECTIONS_MAX_REAUTH_MS, MEDIUM, BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC)
            .define(BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG, INT, BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE, MEDIUM, BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
            .define(SecurityConfig.SECURITY_PROVIDERS_CONFIG, STRING, null, LOW, SecurityConfig.SECURITY_PROVIDERS_DOC)

            // SSL Configuration
            .define(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, CLASS, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DEFAULT, MEDIUM, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
            .define(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, STRING, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DEFAULT, ConfigDef.ValidString.in(Utils.enumOptions(SslClientAuth.class)), MEDIUM, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC)
            .define(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG, STRING, BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES, LOW, BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC)
            .define(BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_DN_CHANGES_VALUE, LOW, BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC)
            .define(BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_SAN_CHANGES_VALUE, LOW, BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC)
            .define(SslConfigs.SSL_PROTOCOL_CONFIG, STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
            .define(SslConfigs.SSL_PROVIDER_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_PROVIDER_DOC)
            .define(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
            .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, STRING, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, MEDIUM, SslConfigs.SSL_KEYSTORE_TYPE_DOC)
            .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
            .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
            .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEY_PASSWORD_DOC)
            .define(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_KEY_DOC)
            .define(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, STRING, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, MEDIUM, SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC)
            .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, MEDIUM, SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
            .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, MEDIUM, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
            .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
            .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, STRING, null, LOW, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
            .define(SslConfigs.SSL_CIPHER_SUITES_CONFIG, LIST, Collections.emptyList(), MEDIUM, SslConfigs.SSL_CIPHER_SUITES_DOC)
            .define(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CLASS, null, LOW, SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC)

            // Sasl Configuration
            .define(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, MEDIUM, BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC)
            .define(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, LIST, BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS, MEDIUM, BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
            .define(BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
            .define(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG, LIST, BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES, MEDIUM, BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
            .define(SaslConfigs.SASL_JAAS_CONFIG, PASSWORD, null, MEDIUM, SaslConfigs.SASL_JAAS_CONFIG_DOC)
            .define(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
            .define(SaslConfigs.SASL_LOGIN_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_LOGIN_CLASS_DOC)
            .define(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
            .define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, STRING, null, MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
            .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, MEDIUM, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
            .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, MEDIUM, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
            .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, MEDIUM, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
            .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, MEDIUM, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
            .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
            .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
            .define(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
            .define(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
            .define(SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS, INT, null, LOW, SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
            .define(SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS, INT, null, LOW, SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC)
            .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LONG, SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LOW, SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
            .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS, LONG, SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS, LOW, SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, STRING, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, LOW, SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, STRING, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME, LOW, SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, STRING, null, MEDIUM, SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, STRING, null, MEDIUM, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, INT, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, LOW, SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, LIST, null, LOW, SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
            .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, STRING, null, LOW, SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC);
}
