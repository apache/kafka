/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.common.security.JaasUtils;

/**
 * The type of the login context, it should be SERVER for the broker and CLIENT for the clients (i.e. consumer and
 * producer). It provides the login context name which defines the section of the JAAS configuration file to be used
 * for login.
 */
public enum LoginType {
    CLIENT(JaasUtils.LOGIN_CONTEXT_CLIENT),
    SERVER(JaasUtils.LOGIN_CONTEXT_SERVER);

    private final String contextName;

    LoginType(String contextName) {
        this.contextName = contextName;
    }

    public String contextName() {
        return contextName;
    }
}
