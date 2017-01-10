/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.io.IOException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.LoginType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaasUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JaasUtils.class);
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";

    public static final String LOGIN_CONTEXT_SERVER = "KafkaServer";
    public static final String LOGIN_CONTEXT_CLIENT = "KafkaClient";
    public static final String SERVICE_NAME = "serviceName";

    public static final String ZK_SASL_CLIENT = "zookeeper.sasl.client";
    public static final String ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";

    /**
     * Returns a JAAS Configuration object. For loginType SERVER, default Configuration
     * is returned. For loginType CLIENT, if JAAS configuration property
     * {@link SaslConfigs#SASL_JAAS_CONFIG} is specified, the configuration object
     * is created by parsing the property value. Otherwise, the default Configuration
     * is returned.
     * @throws IllegalArgumentException if JAAS configuration property is specified
     * for loginType SERVER
     */
    public static Configuration jaasConfig(LoginType loginType, Map<String, ?> configs) {
        Password jaasConfigArgs = (Password) configs.get(SaslConfigs.SASL_JAAS_CONFIG);
        if (jaasConfigArgs != null) {
            if (loginType == LoginType.SERVER)
                throw new IllegalArgumentException("JAAS config property not supported for server");
            else
                return new JaasConfig(loginType, jaasConfigArgs.value());
        } else
            return defaultJaasConfig(loginType);
    }

    private static Configuration defaultJaasConfig(LoginType loginType) {
        String jaasConfigFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        if (jaasConfigFile == null) {
            LOG.debug("System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' and Kafka SASL property '" +
                      SaslConfigs.SASL_JAAS_CONFIG + "' are not set, using default JAAS configuration.");
        }

        Configuration jaasConfig = Configuration.getConfiguration();

        String loginContextName = loginType.contextName();
        AppConfigurationEntry[] configEntries = jaasConfig.getAppConfigurationEntry(loginContextName);
        if (configEntries == null) {
            String errorMessage;
            errorMessage = "Could not find a '" + loginContextName + "' entry in the JAAS configuration. System property '" +
                    JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is " + (jaasConfigFile == null ? "not set" : jaasConfigFile);
            throw new IllegalArgumentException(errorMessage);
        }
        return jaasConfig;
    }

    /**
     * Returns the configuration option for <code>key</code> from the server login context
     * of the default JAAS configuration. If login module name is specified, return option value
     * only from that module.
     */
    public static String defaultServerJaasConfigOption(String key, String loginModuleName) throws IOException {
        return jaasConfigOption(Configuration.getConfiguration(), LoginType.SERVER.contextName(), key, loginModuleName);
    }

    /**
     * Returns the configuration option for <code>key</code> from the login context
     * <code>loginContextName</code> of the specified JAAS configuration.
     * If login module name is specified, return option value only from that module.
     */
    public static String jaasConfigOption(Configuration jaasConfig, String loginContextName, String key, String loginModuleName) throws IOException {
        AppConfigurationEntry[] configurationEntries = jaasConfig.getAppConfigurationEntry(loginContextName);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + loginContextName + "' entry in this JAAS configuration.";
            throw new IOException(errorMessage);
        }

        for (AppConfigurationEntry entry: configurationEntries) {
            if (loginModuleName != null && !loginModuleName.equals(entry.getLoginModuleName()))
                continue;
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String) val;
        }
        return null;
    }

    public static String defaultKerberosRealm()
        throws ClassNotFoundException, NoSuchMethodException,
               IllegalArgumentException, IllegalAccessException,
               InvocationTargetException {

        //TODO Find a way to avoid using these proprietary classes as access to Java 9 will block access by default
        //due to the Jigsaw module system

        Object kerbConf;
        Class<?> classRef;
        Method getInstanceMethod;
        Method getDefaultRealmMethod;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
        kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
        getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm",
                                                           new Class[0]);
        return (String) getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
    }

    public static boolean isZkSecurityEnabled() {
        boolean zkSaslEnabled = Boolean.parseBoolean(System.getProperty(ZK_SASL_CLIENT, "true"));
        String zkLoginContextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, "Client");

        boolean isSecurityEnabled;
        try {
            Configuration loginConf = Configuration.getConfiguration();
            isSecurityEnabled = loginConf.getAppConfigurationEntry(zkLoginContextName) != null;
        } catch (Exception e) {
            throw new KafkaException("Exception while loading Zookeeper JAAS login context '" + zkLoginContextName + "'", e);
        }
        if (isSecurityEnabled && !zkSaslEnabled) {
            LOG.error("JAAS configuration is present, but system property " +
                        ZK_SASL_CLIENT + " is set to false, which disables " +
                        "SASL in the ZooKeeper client");
            throw new KafkaException("Exception while determining if ZooKeeper is secure");
        }

        return isSecurityEnabled;
    }
}

