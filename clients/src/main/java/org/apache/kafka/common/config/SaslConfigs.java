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


public class SaslConfigs {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    public static final String SASL_KAFKA_SERVER_REALM = "sasl.kafka.server.realm";
    public static final String SASL_KAFKA_SERVER_DOC = "The sasl kafka server realm. "
        + "Default will be from kafka jaas config";

    public static final String SASL_KERBEROS_KINIT_CMD = "sasl.kerberos.kinit.cmd";
    public static final String SASL_KERBEROS_KINIT_CMD_DOC = "Kerberos kinit command path. "
        + "Default will be /usr/bin/kinit";
    public static final String DEFAULT_KERBEROS_KINIT_CMD = "/usr/bin/kinit";

}
