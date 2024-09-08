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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;

/**
 * The common group coordinator configurations.
 */
public class CoordinatorCommonConfig {
    /** ********* Common Group coordinator configuration ***********/
    public static final String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG = "group.coordinator.rebalance.protocols";
    public static final String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC = "The list of enabled rebalance protocols. Supported protocols: " +
        Arrays.stream(GroupType.values()).map(GroupType::toString).collect(Collectors.joining(",")) + ". " +
        "The " + GroupType.SHARE + " rebalance protocol is in early access and therefore must not be used in production.";
    public static final List<String> GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT =
        Collections.unmodifiableList(Arrays.asList(GroupType.CLASSIC.toString(), GroupType.CONSUMER.toString()));

    public static final ConfigDef COMMON_GROUP_COORDINATOR_CONFIG_DEF = new ConfigDef()
        .define(GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, LIST, GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT,
            ConfigDef.ValidList.in(Utils.enumOptions(GroupType.class)), MEDIUM, GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC);

}
