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

public final class JaasUtils {
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";
    public static final String DISALLOWED_LOGIN_MODULES_CONFIG = "org.apache.kafka.disallowed.login.modules";
    public static final String DISALLOWED_LOGIN_MODULES_DEFAULT =
            "com.sun.security.auth.module.JndiLoginModule,com.sun.security.auth.module.LdapLoginModule";
    public static final String SERVICE_NAME = "serviceName";

    private JaasUtils() {}

}
