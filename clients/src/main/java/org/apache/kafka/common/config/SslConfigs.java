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

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.Utils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.util.Set;

public class SslConfigs {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String PRINCIPAL_BUILDER_CLASS_CONFIG = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String PRINCIPAL_BUILDER_CLASS_DOC = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release. In recent versions,
     *   the config is optional and there is no default.
     */
    // use FQN to avoid import deprecation warning
    @Deprecated
    public static final String DEFAULT_PRINCIPAL_BUILDER_CLASS =
            org.apache.kafka.common.security.auth.DefaultPrincipalBuilder.class.getName();

    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String SSL_PROTOCOL_DOC = "The SSL protocol used to generate the SSLContext. "
            + "Default setting is TLS, which is fine for most cases. "
            + "Allowed values in recent JVMs are TLS, TLSv1.1 and TLSv1.2. SSL, SSLv2 and SSLv3 "
            + "may be supported in older JVMs, but their usage is discouraged due to known security vulnerabilities.";

    public static final String DEFAULT_SSL_PROTOCOL = "TLS";

    public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
    public static final String SSL_PROVIDER_DOC = "The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.";

    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_CIPHER_SUITES_DOC = "A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. "
            + "By default all the available cipher suites are supported.";

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DOC = "The list of protocols enabled for SSL connections.";
    public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";

    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_TYPE_DOC = "The file format of the key store file. "
            + "This is optional for client.";
    public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_LOCATION_DOC = "The location of the key store file. "
        + "This is optional for client and can be used for two-way authentication for client.";

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEYSTORE_PASSWORD_DOC = "The store password for the key store file. "
        + "This is optional for client and only needed if ssl.keystore.location is configured. ";

    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    public static final String SSL_KEY_PASSWORD_DOC = "The password of the private key in the key store file. "
            + "This is optional for client.";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DOC = "The file format of the trust store file.";
    public static final String DEFAULT_SSL_TRUSTSTORE_TYPE = "JKS";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file. ";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file. If a password is not set access to the truststore is still available, but integrity checking is disabled.";

    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC = "The algorithm used by key manager factory for SSL connections. "
            + "Default value is the key manager factory algorithm configured for the Java Virtual Machine.";
    public static final String DEFAULT_SSL_KEYMANGER_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm();

    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC = "The algorithm used by trust manager factory for SSL connections. "
            + "Default value is the trust manager factory algorithm configured for the Java Virtual Machine.";
    public static final String DEFAULT_SSL_TRUSTMANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = "ssl.endpoint.identification.algorithm";
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = "The endpoint identification algorithm to validate server hostname using server certificate. ";
    public static final String DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "https";

    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = "ssl.secure.random.implementation";
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = "The SecureRandom PRNG implementation to use for SSL cryptography operations. ";

    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String SSL_CLIENT_AUTH_CONFIG = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String SSL_CLIENT_AUTH_DOC = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC;

    public static void addClientSslSupport(ConfigDef config) {
        config.define(SslConfigs.SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
                .define(SslConfigs.SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROVIDER_DOC)
                .define(SslConfigs.SSL_CIPHER_SUITES_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, SslConfigs.SSL_CIPHER_SUITES_DOC)
                .define(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
                .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_KEYSTORE_TYPE_DOC)
                .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
                .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
                .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_KEY_PASSWORD_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
                .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
                .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
                .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC);
    }

    public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

    public static final Set<String> NON_RECONFIGURABLE_CONFIGS = Utils.mkSet(
            BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
            SslConfigs.SSL_PROTOCOL_CONFIG,
            SslConfigs.SSL_PROVIDER_CONFIG,
            SslConfigs.SSL_CIPHER_SUITES_CONFIG,
            SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
            SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
}
