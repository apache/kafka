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

import java.util.HashMap;
import java.util.Map;

public class ZkConfig {
    /** ********* Zookeeper Configuration ***********/
    public static final String  ZkConnectProp = "zookeeper.connect";
    public static final String  ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms";
    public static final String  ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms";
    public static final String  ZkEnableSecureAclsProp = "zookeeper.set.acl";
    public static final String  ZkMaxInFlightRequestsProp = "zookeeper.max.in.flight.requests";
    public static final String  ZkSslClientEnableProp = "zookeeper.ssl.client.enable";
    public static final String  ZkClientCnxnSocketProp = "zookeeper.clientCnxnSocket";
    public static final String  ZkSslKeyStoreLocationProp = "zookeeper.ssl.keystore.location";
    public static final String  ZkSslKeyStorePasswordProp = "zookeeper.ssl.keystore.password";
    public static final String  ZkSslKeyStoreTypeProp = "zookeeper.ssl.keystore.type";
    public static final String  ZkSslTrustStoreLocationProp = "zookeeper.ssl.truststore.location";
    public static final String  ZkSslTrustStorePasswordProp = "zookeeper.ssl.truststore.password";
    public static final String  ZkSslTrustStoreTypeProp = "zookeeper.ssl.truststore.type";
    public static final String  ZkSslProtocolProp = "zookeeper.ssl.protocol";
    public static final String  ZkSslEnabledProtocolsProp = "zookeeper.ssl.enabled.protocols";
    public static final String  ZkSslCipherSuitesProp = "zookeeper.ssl.cipher.suites";
    public static final String  ZkSslEndpointIdentificationAlgorithmProp = "zookeeper.ssl.endpoint.identification.algorithm";
    public static final String  ZkSslCrlEnableProp = "zookeeper.ssl.crl.enable";
    public static final String  ZkSslOcspEnableProp = "zookeeper.ssl.ocsp.enable";


    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    public static final Map<String, String> ZkSslConfigToSystemPropertyMap = new HashMap<>();

    static {
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslClientEnableProp, ZKClientConfig.SECURE_CLIENT);
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkClientCnxnSocketProp, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslKeyStoreLocationProp, "zookeeper.ssl.keyStore.location");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslKeyStorePasswordProp, "zookeeper.ssl.keyStore.password");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslKeyStoreTypeProp, "zookeeper.ssl.keyStore.type");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslTrustStoreLocationProp, "zookeeper.ssl.trustStore.location");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslTrustStorePasswordProp, "zookeeper.ssl.trustStore.password");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslTrustStoreTypeProp, "zookeeper.ssl.trustStore.type");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslProtocolProp, "zookeeper.ssl.protocol");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslEnabledProtocolsProp, "zookeeper.ssl.enabledProtocols");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslCipherSuitesProp, "zookeeper.ssl.ciphersuites");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslEndpointIdentificationAlgorithmProp, "zookeeper.ssl.hostnameVerification");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslCrlEnableProp, "zookeeper.ssl.crl");
        ZkSslConfigToSystemPropertyMap.put(ZkConfig.ZkSslOcspEnableProp, "zookeeper.ssl.ocsp");
    }
}
