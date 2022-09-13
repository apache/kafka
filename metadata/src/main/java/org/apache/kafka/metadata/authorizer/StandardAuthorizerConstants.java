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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.security.auth.KafkaPrincipal;


/**
 * A class which encapsulates the configuration and the ACL data owned by StandardAuthorizer.
 *
 * The methods in this class support lockless concurrent access.
 */
class StandardAuthorizerConstants {
    /**
     * The host or name string used in ACLs that match any host or name.
     */
    static final String WILDCARD = "*";

    /**
     * The principal entry used in ACLs that match any principal.
     */
    static final String WILDCARD_PRINCIPAL = "User:*";

    /**
     * A Principal that matches anything.
     */
    static final KafkaPrincipal WILDCARD_KAFKA_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");

    /**
     * The empty string.
     */
    static final String EMPTY_STRING = "";
}
