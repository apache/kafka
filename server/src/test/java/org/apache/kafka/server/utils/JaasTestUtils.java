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
package org.apache.kafka.server.utils;

import org.apache.kafka.server.config.KafkaConfig;

import java.util.Optional;
import java.util.Properties;

public class JaasTestUtils {
    public static Properties saslConfigs(Optional<Properties> saslProperties) {
        Properties result = saslProperties.orElse(new Properties());

        // IBM Kerberos module doesn't support the serviceName JAAS property, hence it needs to be
        // passed as a Kafka property
        if (isIbmSecurity() && !result.containsKey(KafkaConfig.SASL_KERBEROS_SERVICE_NAME_PROP)) {
            result.put(KafkaConfig.SASL_KERBEROS_SERVICE_NAME_PROP, serviceName());
        }

        return result;
    }

    private static boolean isIbmSecurity() {
        // Implement the logic to determine if IBM security is used
        // You may replace this with the actual condition in your code
        return false;
    }

    private static String serviceName() {
        // Implement the logic to get the service name
        // You may replace this with the actual service name in your code
        return "serviceName";
    }
}
