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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Define the dynamic quota configs. Note that these are not normal configurations that exist in properties files. They
 * only exist dynamically in the controller (or ZK, depending on which mode the cluster is running).
 */
public class QuotaConfigs {
    public static final String PRODUCER_BYTE_RATE_OVERRIDE_CONFIG = "producer_byte_rate";
    public static final String CONSUMER_BYTE_RATE_OVERRIDE_CONFIG = "consumer_byte_rate";
    public static final String REQUEST_PERCENTAGE_OVERRIDE_CONFIG = "request_percentage";
    public static final String CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG = "controller_mutation_rate";
    public static final String IP_CONNECTION_RATE_OVERRIDE_CONFIG = "connection_creation_rate";

    public static final String PRODUCER_BYTE_RATE_DOC = "A rate representing the upper bound (bytes/sec) for producer traffic.";
    public static final String CONSUMER_BYTE_RATE_DOC = "A rate representing the upper bound (bytes/sec) for consumer traffic.";
    public static final String REQUEST_PERCENTAGE_DOC = "A percentage representing the upper bound of time spent for processing requests.";
    public static final String CONTROLLER_MUTATION_RATE_DOC = "The rate at which mutations are accepted for the create " +
        "topics request, the create partitions request and the delete topics request. The rate is accumulated by " +
        "the number of partitions created or deleted.";
    public static final String IP_CONNECTION_RATE_DOC = "An int representing the upper bound of connections accepted " +
        "for the specified IP.";

    public static final int IP_CONNECTION_RATE_DEFAULT = Integer.MAX_VALUE;

    private static Set<String> userClientConfigNames = new HashSet<>(Arrays.asList(
        PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, CONSUMER_BYTE_RATE_OVERRIDE_CONFIG,
        REQUEST_PERCENTAGE_OVERRIDE_CONFIG, CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG
    ));

    private static void buildUserClientQuotaConfigDef(ConfigDef configDef) {
        configDef.define(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, ConfigDef.Type.LONG, Long.MAX_VALUE,
            ConfigDef.Importance.MEDIUM, PRODUCER_BYTE_RATE_DOC);

        configDef.define(CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, ConfigDef.Type.LONG, Long.MAX_VALUE,
            ConfigDef.Importance.MEDIUM, CONSUMER_BYTE_RATE_DOC);

        configDef.define(REQUEST_PERCENTAGE_OVERRIDE_CONFIG, ConfigDef.Type.DOUBLE,
            Integer.valueOf(Integer.MAX_VALUE).doubleValue(),
            ConfigDef.Importance.MEDIUM, REQUEST_PERCENTAGE_DOC);

        configDef.define(CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, ConfigDef.Type.DOUBLE,
            Integer.valueOf(Integer.MAX_VALUE).doubleValue(),
            ConfigDef.Importance.MEDIUM, CONTROLLER_MUTATION_RATE_DOC);
    }

    public static boolean isClientOrUserConfig(String name) {
        return userClientConfigNames.contains(name);
    }

    public static ConfigDef userConfigs() {
        ConfigDef configDef = new ConfigDef();
        ScramMechanism.mechanismNames().forEach(mechanismName -> {
            configDef.define(mechanismName, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "User credentials for SCRAM mechanism " + mechanismName);
        });
        buildUserClientQuotaConfigDef(configDef);
        return configDef;
    }

    public static ConfigDef clientConfigs() {
        ConfigDef configDef = new ConfigDef();
        buildUserClientQuotaConfigDef(configDef);
        return configDef;
    }

    public static ConfigDef ipConfigs() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(IP_CONNECTION_RATE_OVERRIDE_CONFIG, ConfigDef.Type.INT, Integer.MAX_VALUE,
            ConfigDef.Range.atLeast(0), ConfigDef.Importance.MEDIUM, IP_CONNECTION_RATE_DOC);
        return configDef;
    }
}
