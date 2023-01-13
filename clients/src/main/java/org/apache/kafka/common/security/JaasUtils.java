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
package org.apache.kafka.common.security;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;

public final class JaasUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JaasUtils.class);
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";
    public static final String DISALLOWED_LOGIN_MODULES_CONFIG = "org.apache.kafka.disallowed.login.modules";
    public static final String DISALLOWED_LOGIN_MODULES_DEFAULT = "com.sun.security.auth.module.JndiLoginModule";
    public static final String SERVICE_NAME = "serviceName";

    public static final String ZK_SASL_CLIENT = "zookeeper.sasl.client";
    public static final String ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";

    private static final String DEFAULT_ZK_LOGIN_CONTEXT_NAME = "Client";
    private static final String DEFAULT_ZK_SASL_CLIENT = "true";

    private JaasUtils() {}

    public static String zkSecuritySysConfigString() {
        String loginConfig = System.getProperty(JAVA_LOGIN_CONFIG_PARAM);
        String clientEnabled = System.getProperty(ZK_SASL_CLIENT, "default:" + DEFAULT_ZK_SASL_CLIENT);
        String contextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, "default:" + DEFAULT_ZK_LOGIN_CONTEXT_NAME);
        return "[" +
                JAVA_LOGIN_CONFIG_PARAM + "=" + loginConfig +
                ", " +
                ZK_SASL_CLIENT + "=" + clientEnabled +
                ", " +
                ZK_LOGIN_CONTEXT_NAME_KEY + "=" + contextName +
                "]";
    }

    public static boolean isZkSaslEnabled() {
        // Technically a client must also check if TLS mutual authentication has been configured,
        // but we will leave that up to the client code to determine since direct connectivity to ZooKeeper
        // has been deprecated in many clients and we don't wish to re-introduce a ZooKeeper jar dependency here.
        boolean zkSaslEnabled = Boolean.parseBoolean(System.getProperty(ZK_SASL_CLIENT, DEFAULT_ZK_SASL_CLIENT));
        String zkLoginContextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, DEFAULT_ZK_LOGIN_CONTEXT_NAME);

        LOG.debug("Checking login config for Zookeeper JAAS context {}", zkSecuritySysConfigString());

        boolean foundLoginConfigEntry;
        try {
            Configuration loginConf = Configuration.getConfiguration();
            foundLoginConfigEntry = loginConf.getAppConfigurationEntry(zkLoginContextName) != null;
        } catch (Exception e) {
            throw new KafkaException("Exception while loading Zookeeper JAAS login context " +
                    zkSecuritySysConfigString(), e);
        }

        if (foundLoginConfigEntry && !zkSaslEnabled) {
            LOG.error("JAAS configuration is present, but system property " +
                        ZK_SASL_CLIENT + " is set to false, which disables " +
                        "SASL in the ZooKeeper client");
            throw new KafkaException("Exception while determining if ZooKeeper is secure " +
                    zkSecuritySysConfigString());
        }

        return foundLoginConfigEntry;
    }
}

