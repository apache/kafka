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

import java.util.Set;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public class SslConfigs {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String SSL_PROTOCOL_DOC = "The SSL protocol used to generate the SSLContext. "
        + "The default is 'TLSv1.3' when running with Java 11 or newer, 'TLSv1.2' otherwise. "
        + "This value should be fine for most use cases. "
        + "Allowed values in recent JVMs are 'TLSv1.2' and 'TLSv1.3'. 'TLS', 'TLSv1.1', 'SSL', 'SSLv2' and 'SSLv3' "
        + "may be supported in older JVMs, but their usage is discouraged due to known security vulnerabilities. "
        + "With the default value for this config and 'ssl.enabled.protocols', clients will downgrade to 'TLSv1.2' if "
        + "the server does not support 'TLSv1.3'. If this config is set to 'TLSv1.2', clients will not use 'TLSv1.3' even "
        + "if it is one of the values in ssl.enabled.protocols and the server only supports 'TLSv1.3'.";

    public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.3";

    public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
    public static final String SSL_PROVIDER_DOC = "The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.";

    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_CIPHER_SUITES_DOC = "A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. "
            + "By default all the available cipher suites are supported.";

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DOC = "The list of protocols enabled for SSL connections. "
        + "The default is 'TLSv1.2,TLSv1.3' when running with Java 11 or newer, 'TLSv1.2' otherwise. With the "
        + "default value for Java 11, clients and servers will prefer TLSv1.3 if both support it and fallback "
        + "to TLSv1.2 otherwise (assuming both support at least TLSv1.2). This default should be fine for most "
        + "cases. Also see the config documentation for `ssl.protocol`.";
    public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.3";

    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_TYPE_DOC = "The file format of the key store file. "
            + "This is optional for client. The values currently supported by the default `ssl.engine.factory.class` are [JKS, PKCS12, PEM].";
    public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";

    public static final String SSL_KEYSTORE_KEY_CONFIG = "ssl.keystore.key";
    public static final String SSL_KEYSTORE_KEY_DOC = "Private key in the format specified by 'ssl.keystore.type'. "
        + "Default SSL engine factory supports only PEM format with PKCS#8 keys. If the key is encrypted, "
        + "key password must be specified using 'ssl.key.password'";

    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG = "ssl.keystore.certificate.chain";
    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = "Certificate chain in the format specified by 'ssl.keystore.type'. "
        + "Default SSL engine factory supports only PEM format with a list of X.509 certificates";

    public static final String SSL_TRUSTSTORE_CERTIFICATES_CONFIG = "ssl.truststore.certificates";
    public static final String SSL_TRUSTSTORE_CERTIFICATES_DOC = "Trusted certificates in the format specified by 'ssl.truststore.type'. "
        + "Default SSL engine factory supports only PEM format with X.509 certificates.";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_LOCATION_DOC = "The location of the key store file. "
        + "This is optional for client and can be used for two-way authentication for client.";

    public static final String SSL_KEYSTORE_ALIAS_CONFIG = "ssl.keystore.alias";
    public static final String SSL_KEYSTORE_ALIAS_DOC = "This is config is used to pick named alias from the keystore to build the SSL engine and authenticate the client with broker. " +
            "This is an optional config and used only when you have multiple keys in the keystore and you need to control which key needs to be presented to server.";


    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEYSTORE_PASSWORD_DOC = "The store password for the key store file. "
        + "This is optional for client and only needed if 'ssl.keystore.location' is configured. "
        + "Key store password is not supported for PEM format.";

    public static final String SSL_KEYSTORE_AS_STRING = "ssl.keystore.as.string";
    public static final String SSL_KEYSTORE_AS_STRING_DOC = "True when using a base64 encoded keystore string";

    public static final String SSL_TRUSTSTORE_AS_STRING = "ssl.truststore.as.string";
    public static final String SSL_TRUSTSTORE_AS_STRING_DOC = "True when using a base64 encoded truststore string";

    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    public static final String SSL_KEY_PASSWORD_DOC = "The password of the private key in the key store file or "
        + "the PEM key specified in 'ssl.keystore.key'.";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DOC = "The file format of the trust store file. The values currently supported by the default `ssl.engine.factory.class` are [JKS, PKCS12, PEM].";
    public static final String DEFAULT_SSL_TRUSTSTORE_TYPE = "JKS";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file.";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file. "
        + "If a password is not set, trust store file configured will still be used, but integrity checking is disabled. "
        + "Trust store password is not supported for PEM format.";

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

    public static final String SSL_ENGINE_FACTORY_CLASS_CONFIG = "ssl.engine.factory.class";
    public static final String SSL_ENGINE_FACTORY_CLASS_DOC = "The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine objects. "
        + "Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory. "
        + "Alternatively, setting this to org.apache.kafka.common.security.ssl.CommonNameLoggingSslEngineFactory will log the common name of expired SSL certificates used by clients to authenticate at any of the brokers with log level "
        + LogLevelConfig.INFO_LOG_LEVEL + ". "
        + "Note that this will cause a tiny delay during establishment of new connections from mTLS clients to brokers due to the extra code for examining the certificate chain provided by the client. "
        + "Note further that the implementation uses a custom truststore based on the standard Java truststore and thus might be considered a security risk due to not being as mature as the standard one.";

    public static void addClientSslSupport(ConfigDef config) {
        config.define(SslConfigs.SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
                .define(SslConfigs.SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROVIDER_DOC)
                .define(SslConfigs.SSL_CIPHER_SUITES_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, SslConfigs.SSL_CIPHER_SUITES_DOC)
                .define(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
                .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_KEYSTORE_TYPE_DOC)
                .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
                .define(SslConfigs.SSL_KEYSTORE_ALIAS_CONFIG, ConfigDef.Type.STRING, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_ALIAS_DOC)
                .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
                .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_KEY_PASSWORD_DOC)
                .define(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, ConfigDef.Type.PASSWORD, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_KEY_DOC)
                .define(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, ConfigDef.Type.PASSWORD, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, ConfigDef.Type.PASSWORD, null,  ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ConfigDef.Importance.MEDIUM, SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
                .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
                .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ConfigDef.Importance.LOW, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
                .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
                .define(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC)
                .define(SslConfigs.SSL_KEYSTORE_AS_STRING, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,SslConfigs.SSL_KEYSTORE_AS_STRING_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_AS_STRING, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,SslConfigs.SSL_TRUSTSTORE_AS_STRING_DOC);
    }

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
            SslConfigs.SSL_KEYSTORE_KEY_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);

    public static final Set<String> NON_RECONFIGURABLE_CONFIGS = Set.of(
            BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
            SslConfigs.SSL_PROTOCOL_CONFIG,
            SslConfigs.SSL_PROVIDER_CONFIG,
            SslConfigs.SSL_CIPHER_SUITES_CONFIG,
            SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
            SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
            SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG);
}
