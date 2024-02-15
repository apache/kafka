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

import org.apache.zookeeper.client.ZKClientConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ZkConfig {
    /** ********* Zookeeper Configuration ***********/
    public static final String ZK_CONNECT_PROP = "zookeeper.connect";
    public static final String ZK_SESSION_TIMEOUT_MS_PROP = "zookeeper.session.timeout.ms";
    public static final String ZK_CONNECTION_TIMEOUT_MS_PROP = "zookeeper.connection.timeout.ms";
    public static final String ZK_ENABLE_SECURE_ACLS_PROP = "zookeeper.set.acl";
    public static final String ZK_MAX_IN_FLIGHT_REQUESTS_PROP = "zookeeper.max.in.flight.requests";
    public static final String ZK_SSL_CLIENT_ENABLE_PROP = "zookeeper.ssl.client.enable";
    public static final String ZK_CLIENT_CNXN_SOCKET_PROP = "zookeeper.clientCnxnSocket";
    public static final String ZK_SSL_KEY_STORE_LOCATION_PROP = "zookeeper.ssl.keystore.location";
    public static final String ZK_SSL_KEY_STORE_PASSWORD_PROP = "zookeeper.ssl.keystore.password";
    public static final String ZK_SSL_KEY_STORE_TYPE_PROP = "zookeeper.ssl.keystore.type";
    public static final String ZK_SSL_TRUST_STORE_LOCATION_PROP = "zookeeper.ssl.truststore.location";
    public static final String ZK_SSL_TRUST_STORE_PASSWORD_PROP = "zookeeper.ssl.truststore.password";
    public static final String ZK_SSL_TRUST_STORE_TYPE_PROP = "zookeeper.ssl.truststore.type";
    public static final String ZK_SSL_PROTOCOL_PROP = "zookeeper.ssl.protocol";
    public static final String ZK_SSL_ENABLED_PROTOCOLS_PROP = "zookeeper.ssl.enabled.protocols";
    public static final String ZK_SSL_CIPHER_SUITES_PROP = "zookeeper.ssl.cipher.suites";
    public static final String ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP = "zookeeper.ssl.endpoint.identification.algorithm";
    public static final String ZK_SSL_CRL_ENABLE_PROP = "zookeeper.ssl.crl.enable";
    public static final String ZK_SSL_OCSP_ENABLE_PROP = "zookeeper.ssl.ocsp.enable";

    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    public static final Map<String, String> ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP;

    static {
        Map<String, String> zkSslConfigToSystemPropertyMap = new HashMap<>();

        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CLIENT_ENABLE_PROP, ZKClientConfig.SECURE_CLIENT);
        zkSslConfigToSystemPropertyMap.put(ZK_CLIENT_CNXN_SOCKET_PROP, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_KEY_STORE_LOCATION_PROP, "zookeeper.ssl.keyStore.location");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_KEY_STORE_PASSWORD_PROP, "zookeeper.ssl.keyStore.password");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_KEY_STORE_TYPE_PROP, "zookeeper.ssl.keyStore.type");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_TRUST_STORE_LOCATION_PROP, "zookeeper.ssl.trustStore.location");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_TRUST_STORE_PASSWORD_PROP, "zookeeper.ssl.trustStore.password");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_TRUST_STORE_TYPE_PROP, "zookeeper.ssl.trustStore.type");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_PROTOCOL_PROP, "zookeeper.ssl.protocol");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_ENABLED_PROTOCOLS_PROP, "zookeeper.ssl.enabledProtocols");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CIPHER_SUITES_PROP, "zookeeper.ssl.ciphersuites");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP, "zookeeper.ssl.hostnameVerification");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_CRL_ENABLE_PROP, "zookeeper.ssl.crl");
        zkSslConfigToSystemPropertyMap.put(ZK_SSL_OCSP_ENABLE_PROP, "zookeeper.ssl.ocsp");

        ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP = Collections.unmodifiableMap(zkSslConfigToSystemPropertyMap);
    }
}
