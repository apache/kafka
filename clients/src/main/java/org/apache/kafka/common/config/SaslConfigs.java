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

import java.util.Collections;
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

    public static final String SASL_ENABLED_MECHANISMS = "sasl.enabled.mechanisms";
    public static final String SASL_ENABLED_MECHANISMS_DOC = "The list of SASL mechanisms enabled in the Kafka server. "
        + "The list may contain any mechanism for which a security provider is available. "
        + "Only GSSAPI is enabled by default.";
    public static final List<String> DEFAULT_SASL_ENABLED_MECHANISMS = Collections.singletonList(GSSAPI_MECHANISM);

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

    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = "sasl.kerberos.principal.to.local.rules";
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = "A list of rules for mapping from principal names to short names (typically operating system usernames). " +
            "The rules are evaluated in order and the first rule that matches a principal name is used to map it to a short name. Any later rules in the list are ignored. " +
            "By default, principal names of the form {username}/{hostname}@{REALM} are mapped to {username}. " +
            "For more details on the format please see <a href=\"#security_authz\"> security authorization and acls</a>.";
    public static final List<String> DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = Collections.singletonList("DEFAULT");

    public static void addClientSaslSupport(ConfigDef config) {
        config.define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
                .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
                .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Type.DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
                .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Type.LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, ConfigDef.Importance.LOW, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
                .define(SaslConfigs.SASL_MECHANISM, ConfigDef.Type.STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, ConfigDef.Importance.MEDIUM, SaslConfigs.SASL_MECHANISM_DOC);
    }
}
