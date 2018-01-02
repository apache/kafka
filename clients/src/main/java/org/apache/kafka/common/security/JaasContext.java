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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class JaasContext {

    private static final Logger LOG = LoggerFactory.getLogger(JaasUtils.class);

    private static final String GLOBAL_CONTEXT_NAME_SERVER = "KafkaServer";
    private static final String GLOBAL_CONTEXT_NAME_CLIENT = "KafkaClient";

    /**
     * Returns an instance of this class.
     *
     * For contextType SERVER, the context will contain the default Configuration and the context name will be one of:
     *
     * 1. Lowercased listener name followed by a period and the string `KafkaServer`
     * 2. The string `KafkaServer`
     *
     * If both are valid entries in the JAAS configuration, the first option is chosen.
     *
     * For contextType CLIENT, if JAAS configuration property @link SaslConfigs#SASL_JAAS_CONFIG} is specified,
     * the configuration object is created by parsing the property value. Otherwise, the default Configuration
     * is returned. The context name is always `KafkaClient`.
     *
     * @throws IllegalArgumentException if JAAS configuration property is specified for contextType SERVER, if
     * listenerName is not defined for contextType SERVER of if listenerName is defined for contextType CLIENT.
     */
    public static JaasContext load(JaasContext.Type contextType, ListenerName listenerName,
                                   Map<String, ?> configs) {
        String listenerContextName;
        String globalContextName;
        switch (contextType) {
            case CLIENT:
                if (listenerName != null)
                    throw new IllegalArgumentException("listenerName should be null for CLIENT");
                globalContextName = GLOBAL_CONTEXT_NAME_CLIENT;
                listenerContextName = null;
                break;
            case SERVER:
                if (listenerName == null)
                    throw new IllegalArgumentException("listenerName should not be null for SERVER");
                globalContextName = GLOBAL_CONTEXT_NAME_SERVER;
                listenerContextName = listenerName.value().toLowerCase(Locale.ROOT) + "." + GLOBAL_CONTEXT_NAME_SERVER;
                break;
            default:
                throw new IllegalArgumentException("Unexpected context type " + contextType);
        }
        return load(contextType, listenerContextName, globalContextName, configs);
    }

    static JaasContext load(JaasContext.Type contextType, String listenerContextName,
                            String globalContextName, Map<String, ?> configs) {
        Password jaasConfigArgs = (Password) configs.get(SaslConfigs.SASL_JAAS_CONFIG);
        if (jaasConfigArgs != null) {
            if (contextType == JaasContext.Type.SERVER)
                throw new IllegalArgumentException("JAAS config property not supported for server");
            else {
                JaasConfig jaasConfig = new JaasConfig(globalContextName, jaasConfigArgs.value());
                AppConfigurationEntry[] clientModules = jaasConfig.getAppConfigurationEntry(globalContextName);
                int numModules = clientModules == null ? 0 : clientModules.length;
                if (numModules != 1)
                    throw new IllegalArgumentException("JAAS config property contains " + numModules + " login modules, should be 1 module");
                return new JaasContext(globalContextName, contextType, jaasConfig);
            }
        } else
            return defaultContext(contextType, listenerContextName, globalContextName);
    }

    private static JaasContext defaultContext(JaasContext.Type contextType, String listenerContextName,
                                              String globalContextName) {
        String jaasConfigFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        if (jaasConfigFile == null) {
            if (contextType == Type.CLIENT) {
                LOG.debug("System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' and Kafka SASL property '" +
                        SaslConfigs.SASL_JAAS_CONFIG + "' are not set, using default JAAS configuration.");
            } else {
                LOG.debug("System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is not set, using default JAAS " +
                        "configuration.");
            }
        }

        Configuration jaasConfig = Configuration.getConfiguration();

        AppConfigurationEntry[] configEntries = null;
        String contextName = globalContextName;

        if (listenerContextName != null) {
            configEntries = jaasConfig.getAppConfigurationEntry(listenerContextName);
            if (configEntries != null)
                contextName = listenerContextName;
        }

        if (configEntries == null)
            configEntries = jaasConfig.getAppConfigurationEntry(globalContextName);

        if (configEntries == null) {
            String listenerNameText = listenerContextName == null ? "" : " or '" + listenerContextName + "'";
            String errorMessage = "Could not find a '" + globalContextName + "'" + listenerNameText + " entry in the JAAS " +
                    "configuration. System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is " +
                    (jaasConfigFile == null ? "not set" : jaasConfigFile);
            throw new IllegalArgumentException(errorMessage);
        }

        return new JaasContext(contextName, contextType, jaasConfig);
    }

    /**
     * The type of the SASL login context, it should be SERVER for the broker and CLIENT for the clients (consumer, producer,
     * etc.). This is used to validate behaviour (e.g. some functionality is only available in the broker or clients).
     */
    public enum Type { CLIENT, SERVER; }

    private final String name;
    private final Type type;
    private final Configuration configuration;
    private final List<AppConfigurationEntry> configurationEntries;

    public JaasContext(String name, Type type, Configuration configuration) {
        this.name = name;
        this.type = type;
        this.configuration = configuration;
        AppConfigurationEntry[] entries = configuration.getAppConfigurationEntry(name);
        if (entries == null)
            throw new IllegalArgumentException("Could not find a '" + name + "' entry in this JAAS configuration.");
        this.configurationEntries = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(entries)));
    }

    public String name() {
        return name;
    }

    public Type type() {
        return type;
    }

    public Configuration configuration() {
        return configuration;
    }

    public List<AppConfigurationEntry> configurationEntries() {
        return configurationEntries;
    }

    /**
     * Returns the configuration option for <code>key</code> from this context.
     * If login module name is specified, return option value only from that module.
     */
    public String configEntryOption(String key, String loginModuleName) {
        for (AppConfigurationEntry entry : configurationEntries) {
            if (loginModuleName != null && !loginModuleName.equals(entry.getLoginModuleName()))
                continue;
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String) val;
        }
        return null;
    }

}
