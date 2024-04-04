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

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;

public class KafkaConfig {

    /** ********* SSL Configuration ****************/
    public final static String SSL_PROTOCOL_CONFIG = SslConfigs.SSL_PROTOCOL_CONFIG;
    public final static String SSL_PROVIDER_CONFIG = SslConfigs.SSL_PROVIDER_CONFIG;
    public final static String SSL_CIPHER_SUITES_CONFIG = SslConfigs.SSL_CIPHER_SUITES_CONFIG;
    public final static String SSL_ENABLED_PROTOCOLS_CONFIG = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
    public final static String SSL_KEYSTORE_TYPE_CONFIG = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    public final static String SSL_KEYSTORE_LOCATION_CONFIG = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
    public final static String SSL_KEYSTORE_PASSWORD_CONFIG = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
    public final static String SSL_KEY_PASSWORD_CONFIG = SslConfigs.SSL_KEY_PASSWORD_CONFIG;
    public final static String SSL_KEYSTORE_KEY_CONFIG = SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
    public final static String SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
    public final static String SSL_TRUSTSTORE_TYPE_CONFIG = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
    public final static String SSL_TRUSTSTORE_LOCATION_CONFIG = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
    public final static String SSL_TRUSTSTORE_PASSWORD_CONFIG = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
    public final static String SSL_TRUSTSTORE_CERTIFICATES_CONFIG = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
    public final static String SSL_KEYMANAGER_ALGORITHM_CONFIG = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
    public final static String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
    public final static String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
    public final static String SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
    public final static String SSL_CLIENT_AUTH_CONFIG = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG;
    public final static String SSL_PRINCIPAL_MAPPING_RULES_CONFIG = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG;
    public final static String SSL_ENGINE_FACTORY_CLASS_CONFIG = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
    public final static String SSL_ALLOW_DN_CHANGES_CONFIG = BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG;
    public final static String SSL_ALLOW_SAN_CHANGES_CONFIG = BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG;

    /** ********* SASL Configuration ****************/
    public final static String SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG = "sasl.mechanism.inter.broker.protocol";
    public final static String SASL_JAAS_CONFIG = SaslConfigs.SASL_JAAS_CONFIG;
    public final static String SASL_ENABLED_MECHANISMS_CONFIG = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
    public final static String SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS;
    public final static String SASL_CLIENT_CALLBACK_HANDLER_CLASS_CONFIG = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
    public final static String SASL_LOGIN_CLASS_CONFIG = SaslConfigs.SASL_LOGIN_CLASS;
    public final static String SASL_LOGIN_CALLBACK_HANDLER_CLASS_CONFIG = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
    public final static String SASL_KERBEROS_SERVICE_NAME_CONFIG = SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
    public final static String SASL_KERBEROS_KINIT_CMD_CONFIG = SaslConfigs.SASL_KERBEROS_KINIT_CMD;
    public final static String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;
    public final static String SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER;
    public final static String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN;
    public final static String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG;
    public final static String SASL_LOGIN_REFRESH_WINDOW_FACTOR_CONFIG = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR;
    public final static String SASL_LOGIN_REFRESH_WINDOW_JITTER_CONFIG = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER;
    public final static String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_CONFIG = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
    public final static String SASL_LOGIN_REFRESH_BUFFER_SECONDS_CONFIG = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS;

    public final static String SASL_LOGIN_CONNECT_TIMEOUT_MS_CONFIG = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
    public final static String SASL_LOGIN_READ_TIMEOUT_MS_CONFIG = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_CONFIG = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MS_CONFIG = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
    public final static String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_CONFIG = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
    public final static String SASL_OAUTHBEARER_SUB_CLAIM_NAME_CONFIG = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
    public final static String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_CONFIG = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_CONFIG = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_CONFIG = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_CONFIG = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_CONFIG = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
    public final static String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_CONFIG = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
    public final static String SASL_OAUTHBEARER_EXPECTED_AUDIENCE_CONFIG = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
    public final static String SASL_OAUTHBEARER_EXPECTED_ISSUER_CONFIG = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;

    /** ******** Common Security Configuration *************/
    public final static String PRINCIPAL_BUILDER_CLASS_CONFIG = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG;
    public final static String CONNECTIONS_MAX_REAUTH_MS_CONFIG = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS;
    public final static String SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG;
    public final static String SECURITY_PROVIDER_CLASS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;

    /** ********* SSL Configuration ****************/
    public final static String SSL_PROTOCOL_DOC = SslConfigs.SSL_PROTOCOL_DOC;
    public final static String SSL_PROVIDER_DOC = SslConfigs.SSL_PROVIDER_DOC;
    public final static String SSL_CIPHER_SUITES_DOC = SslConfigs.SSL_CIPHER_SUITES_DOC;
    public final static String SSL_ENABLED_PROTOCOLS_DOC = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC;
    public final static String SSL_KEYSTORE_TYPE_DOC = SslConfigs.SSL_KEYSTORE_TYPE_DOC;
    public final static String SSL_KEYSTORE_LOCATION_DOC = SslConfigs.SSL_KEYSTORE_LOCATION_DOC;
    public final static String SSL_KEYSTORE_PASSWORD_DOC = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC;
    public final static String SSL_KEY_PASSWORD_DOC = SslConfigs.SSL_KEY_PASSWORD_DOC;
    public final static String SSL_KEYSTORE_KEY_DOC = SslConfigs.SSL_KEYSTORE_KEY_DOC;
    public final static String SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC;
    public final static String SSL_TRUSTSTORE_TYPE_DOC = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC;
    public final static String SSL_TRUSTSTORE_PASSWORD_DOC = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;
    public final static String SSL_TRUSTSTORE_LOCATION_DOC = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC;
    public final static String SSL_TRUSTSTORE_CERTIFICATES_DOC = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC;
    public final static String SSL_KEYMANAGER_ALGORITHM_DOC = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC;
    public final static String SSL_TRUSTMANAGER_ALGORITHM_DOC = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC;
    public final static String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;
    public final static String SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC;
    public final static String SSL_CLIENT_AUTH_DOC = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC;
    public final static String SSL_PRINCIPAL_MAPPING_RULES_DOC = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC;
    public final static String SSL_ENGINE_FACTORY_CLASS_DOC = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC;
    public final static String SSL_ALLOW_DN_CHANGES_DOC = BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC;
    public final static String SSL_ALLOW_SAN_CHANGES_DOC = BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC;

    /** ********* Sasl Configuration ****************/
    public final static String SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC = "SASL mechanism used for inter-broker communication. Default is GSSAPI.";
    public final static String SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;
    public final static String SASL_ENABLED_MECHANISMS_DOC = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC;
    public final static String SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC;
    public final static String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC;
    public final static String SASL_LOGIN_CLASS_DOC = SaslConfigs.SASL_LOGIN_CLASS_DOC;
    public final static String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC;
    public final static String SASL_KERBEROS_SERVICE_NAME_DOC = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC;
    public final static String SASL_KERBEROS_KINIT_CMD_DOC = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC;
    public final static String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC;
    public final static String SASL_KERBEROS_TICKET_RENEW_JITTER_DOC = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC;
    public final static String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC;
    public final static String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC;
    public final static String SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC;
    public final static String SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC;
    public final static String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC;
    public final static String SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC;
    public final static String SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC;
    public final static String SASL_LOGIN_READ_TIMEOUT_MS_DOC = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC;
    public final static String SASL_LOGIN_RETRY_BACKOFF_MS_DOC = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC;
    public final static String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC;
    public final static String SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC;
    public final static String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC;
    public final static String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC;
    public final static String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC;
    public final static String SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC;
    public final static String SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC;

    /** ******** Common Security Configuration ************ */
    public final static String PRINCIPAL_BUILDER_CLASS_DOC = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC;
    public final static String CONNECTIONS_MAX_REAUTH_MS_DOC = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC;
    public final static String SASL_SERVER_MAX_RECEIVE_SIZE_DOC = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC;
    public final static String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

}
