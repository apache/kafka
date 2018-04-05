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

import java.util.List;

public class SaslConfigs {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */
    /** SASL mechanism configuration - standard mechanism names are listed <a href="http://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml">here</a>. */
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SASL_MECHANISM_DOC = "SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism.";
    public static final String GSSAPI_MECHANISM = "GSSAPI";
    public static final String DEFAULT_SASL_MECHANISM = GSSAPI_MECHANISM;

    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String SASL_ENABLED_MECHANISMS = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String SASL_ENABLED_MECHANISMS_DOC = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final List<String> DEFAULT_SASL_ENABLED_MECHANISMS = BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS;

    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_JAAS_CONFIG_DOC = "JAAS login context parameters for SASL connections in the format used by JAAS configuration files. "
        + "JAAS configuration file format is described <a href=\"http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html\">here</a>. "
        + "The format for the value is: '<loginModuleClass> <controlFlag> (<optionName>=<optionValue>)*;'. For brokers, "
        + "the config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example, "
        + "listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=com.example.ScramLoginModule required;";

    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class";
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL client callback handler class "
        + "that implements the AuthenticateCallbackHandler interface.";

    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL login callback handler class "
            + "that implements the AuthenticateCallbackHandler interface. For brokers, login callback handler config must be prefixed with "
            + "listener prefix and SASL mechanism name in lower-case. For example, "
            + "listener.name.sasl_ssl.scram-sha-256.sasl.login.callback.handler.class=com.example.CustomScramLoginCallbackHandler";

    public static final String SASL_LOGIN_CLASS = "sasl.login.class";
    public static final String SASL_LOGIN_CLASS_DOC = "The fully qualified name of a class that implements the Login interface. "
        + "For brokers, login config must be prefixed with listener prefix and SASL mechanism name in lower-case. For example, "
        + "listener.name.sasl_ssl.scram-sha-256.sasl.login.class=com.example.CustomScramLogin";

    public static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    public static final String SASL_KERBEROS_SERVICE_NAME_DOC = "The Kerberos principal name that Kafka runs as. "
        + "This can be defined either in Kafka's JAAS config or in Kafka's config.";

    public static final String SASL_KERBEROS_KINIT_CMD = "sasl.kerberos.kinit.cmd";
    public static final String SASL_KERBEROS_KINIT_CMD_DOC = "Kerberos kinit command path.";
    public static final String DEFAULT_KERBEROS_KINIT_CMD = "/usr/bin/kinit";

    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = "sasl.kerberos.ticket.renew.window.factor";
    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC = "Login thread will sleep until the specified window factor of time from last refresh"
        + " to ticket's expiry has been reached, at which time it will try to renew the ticket.";
    public static final double DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = 0.80;

    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER = "sasl.kerberos.ticket.renew.jitter";
    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER_DOC = "Percentage of random jitter added to the renewal time.";
    public static final double DEFAULT_KERBEROS_TICKET_RENEW_JITTER = 0.05;

    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN = "sasl.kerberos.min.time.before.relogin";
    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC = "Login thread sleep time between refresh attempts.";
    public static final long DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;

    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC;
    /**
     * @deprecated As of 1.0.0. This field will be removed in a future major release.
     */
    @Deprecated
    public static final List<String> DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES;

    public static void addClientSaslSupport(ConfigDef config) {
        config.define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
                .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
                .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Type.LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
                .define(SaslConfigs.SASL_MECHANISM, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_MECHANISM_DOC)
                .define(SaslConfigs.SASL_JAAS_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_JAAS_CONFIG_DOC)
                .define(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_LOGIN_CLASS_DOC);
    }
}
