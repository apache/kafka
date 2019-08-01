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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.security.Security;
import java.util.Map;

/**
 * Contains the common security config for SSL and SASL
 */
public class SecurityConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityConfig.class);

    public static final String SECURITY_PROVIDER_CLASS_CONFIG = "security.provider.class";
    public static final String SECURITY_PROVIDER_CLASS_DOC = "Java Provider to register security algorithms";

    public static void addConfiguredSecurityProviders(Map<String, ?> configs) {
        String securityProviderClassesStr = (String) configs.get(SecurityConfig.SECURITY_PROVIDER_CLASS_CONFIG);
        if (securityProviderClassesStr == null || securityProviderClassesStr.equals("")) {
            return;
        }
        try {
            String[] securityProviderClasses = securityProviderClassesStr.replaceAll("\\s+", "").split(",");
            for (int index = 0; index < securityProviderClasses.length; index++) {
                Security.insertProviderAt((Provider) Class.forName(securityProviderClasses[index]).newInstance(), index + 1);
            }
        } catch (ClassNotFoundException cnfe) {
            LOGGER.error("Unrecognized security provider class", cnfe);
        } catch (IllegalAccessException | InstantiationException e) {
            LOGGER.error("Unexpected implementation of security provider class", e);
        }
    }
}
