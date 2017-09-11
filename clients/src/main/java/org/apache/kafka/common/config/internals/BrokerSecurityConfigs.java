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
package org.apache.kafka.common.config.internals;

import java.util.Collections;
import java.util.List;

/**
 * Common home for broker-side security configs which need to be accessible from the libraries shared
 * between the broker and the client.
 *
 * Note this is an internal API and subject to change without notice.
 */
public class BrokerSecurityConfigs {

    public static final String PRINCIPAL_BUILDER_CLASS_CONFIG = "principal.builder.class";
    public static final String PRINCIPAL_BUILDER_CLASS_DOC = "The fully qualified name of a class that implements the " +
            "KafkaPrincipalBuilder interface, which is used to build the KafkaPrincipal object used during " +
            "authorization. This config also supports the deprecated PrincipalBuilder interface which was previously " +
            "used for connections with the SSL SecurityProtocol.";

    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG = "sasl.kerberos.principal.to.local.rules";
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = "A list of rules for mapping from principal " +
            "names to short names (typically operating system usernames). The rules are evaluated in order and the " +
            "first rule that matches a principal name is used to map it to a short name. Any later rules in the list are " +
            "ignored. By default, principal names of the form {username}/{hostname}@{REALM} are mapped to {username}. " +
            "For more details on the format please see <a href=\"#security_authz\"> security authorization and acls</a>. " +
            "Note that this configuration is ignored if an extension of KafkaPrincipalBuilder is provided by the " +
            "<code>" + PRINCIPAL_BUILDER_CLASS_CONFIG + "</code> configuration.";
    public static final List<String> DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = Collections.singletonList("DEFAULT");

}
