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

import org.apache.kafka.common.config.ConfigDef.Range;

public class SaslConfigs {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */
    /** SASL mechanism configuration - standard mechanism names are listed <a href="http://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml">here</a>. */
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SASL_MECHANISM_DOC = "SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism.";
    public static final String GSSAPI_MECHANISM = "GSSAPI";
    public static final String DEFAULT_SASL_MECHANISM = GSSAPI_MECHANISM;

    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_JAAS_CONFIG_DOC = "JAAS login context parameters for SASL connections in the format used by JAAS configuration files. "
        + "JAAS configuration file format is described <a href=\"http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html\">here</a>. "
        + "The format for the value is: <code>loginModuleClass controlFlag (optionName=optionValue)*;</code>. For brokers, "
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

    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR = "sasl.login.refresh.window.factor";
    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC = "Login refresh thread will sleep until the specified window factor relative to the"
            + " credential's lifetime has been reached, at which time it will try to refresh the credential."
            + " Legal values are between 0.5 (50%) and 1.0 (100%) inclusive; a default value of 0.8 (80%) is used"
            + " if no value is specified. Currently applies only to OAUTHBEARER.";
    public static final double DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR = 0.80;

    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER = "sasl.login.refresh.window.jitter";
    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC = "The maximum amount of random jitter relative to the credential's lifetime"
            + " that is added to the login refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) inclusive;"
            + " a default value of 0.05 (5%) is used if no value is specified. Currently applies only to OAUTHBEARER.";
    public static final double DEFAULT_LOGIN_REFRESH_WINDOW_JITTER = 0.05;

    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS = "sasl.login.refresh.min.period.seconds";
    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC = "The desired minimum time for the login refresh thread to wait before refreshing a credential,"
            + " in seconds. Legal values are between 0 and 900 (15 minutes); a default value of 60 (1 minute) is used if no value is specified.  This value and "
            + " sasl.login.refresh.buffer.seconds are both ignored if their sum exceeds the remaining lifetime of a credential."
            + " Currently applies only to OAUTHBEARER.";
    public static final short DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS = 60;

    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS = "sasl.login.refresh.buffer.seconds";
    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC = "The amount of buffer time before credential expiration to maintain when refreshing a credential,"
            + " in seconds. If a refresh would otherwise occur closer to expiration than the number of buffer seconds then the refresh will be moved up to maintain"
            + " as much of the buffer time as possible. Legal values are between 0 and 3600 (1 hour); a default value of  300 (5 minutes) is used if no value is specified."
            + " This value and sasl.login.refresh.min.period.seconds are both ignored if their sum exceeds the remaining lifetime of a credential."
            + " Currently applies only to OAUTHBEARER.";
    public static final short DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS = 300;

    public static void addClientSaslSupport(ConfigDef config) {
        config.define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
                .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
                .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Type.LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, Range.between(0.5, 1.0), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER, Range.between(0.0, 0.25), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, ConfigDef.Type.SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, Range.between(0, 900), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
                .define(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, ConfigDef.Type.SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, Range.between(0, 3600), ConfigDef.Importance.LOW, SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
                .define(SaslConfigs.SASL_MECHANISM, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_MECHANISM_DOC)
                .define(SaslConfigs.SASL_JAAS_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_JAAS_CONFIG_DOC)
                .define(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
                .define(SaslConfigs.SASL_LOGIN_CLASS, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_LOGIN_CLASS_DOC);
    }
}
