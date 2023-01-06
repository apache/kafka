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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The consumer configuration keys
 */
public class ReverseConsumerConfig extends ConsumerConfig {
    protected static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>jumpback.offset.count</code>
     */
    public static final String JUMPBACK_OFFSET_COUNT_CONFIG = "jumpback.offset.count";
    private static final String JUMPBACK_OFFSET_COUNT_DOC = "The number of offsets to jump back per partition when polling.";

    static {
        CONFIG = ConsumerConfig.CONFIG.define(JUMPBACK_OFFSET_COUNT_CONFIG,
                Type.INT,
                500,
                Importance.HIGH,
                JUMPBACK_OFFSET_COUNT_DOC);
    }

    public ReverseConsumerConfig(Properties props) {
        super(props);
    }

    public ReverseConsumerConfig(Map<String, Object> props) {
        super(props);
    }

    protected ReverseConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "reverseconsumerconfigs_" + config));
    }

}
