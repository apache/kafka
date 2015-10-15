/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.config;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public class SSLConfigs {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    public static final String PRINCIPAL_BUILDER_CLASS_CONFIG = "principal.builder.class";
    public static final String PRINCIPAL_BUILDER_CLASS_DOC = "principal builder to generate a java Principal. This config is optional for client.";
    public static final String DEFAULT_PRINCIPAL_BUILDER_CLASS = "org.apache.kafka.common.security.auth.DefaultPrincipalBuilder";

    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String SSL_PROTOCOL_DOC = "The ssl protocol used to generate SSLContext."
            + "Default setting is TLS. Allowed values are SSL, SSLv2, SSLv3, TLS, TLSv1.1, TLSv1.2";
    public static final String DEFAULT_SSL_PROTOCOL = "TLS";

    public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
    public static final String SSL_PROVIDER_DOC = "The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.";

    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_CIPHER_SUITES_DOC = "A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol."
            + "By default all the available cipher suites are supported.";

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DOC = "The list of protocols enabled for SSL connections. "
            + "All versions of TLS is enabled by default.";
    public static final String DEFAULT_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";

    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_TYPE_DOC = "The file format of the key store file. "
            + "This is optional for client. Default value is JKS";
    public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_LOCATION_DOC = "The location of the key store file. "
        + "This is optional for Client and can be used for two-way authentication for client.";

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEYSTORE_PASSWORD_DOC = "The store password for the key store file."
        + "This is optional for client and only needed if the ssl.keystore.location configured. ";

    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    public static final String SSL_KEY_PASSWORD_DOC = "The password of the private key in the key store file. "
            + "This is optional for client.";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DOC = "The file format of the trust store file. "
            + "Default value is JKS.";
    public static final String DEFAULT_SSL_TRUSTSTORE_TYPE = "JKS";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file. ";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file. ";

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

    public static final String SSL_CLIENT_AUTH_CONFIG = "ssl.client.auth";
    public static final String SSL_CLIENT_AUTH_DOC = "Configures kafka broker to request client authentication."
                                           + " The following settings are common: "
                                           + " <ul>"
                                           + " <li><code>ssl.want.client.auth=required</code> If set to required"
                                           + " client authentication is required."
                                           + " <li><code>ssl.client.auth=requested</code> This means client authentication is optional."
                                           + " unlike requested , if this option is set client can choose not to provide authentication information about itself"
                                           + " <li><code>ssl.client.auth=none</code> This means client authentication is not needed.";

}
