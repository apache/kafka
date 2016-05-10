/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration needed for all sink connectors
 */

public class SinkConnectorConfig extends ConnectorConfig {

    public static final String TOPICS_CONFIG = "topics";
    private static final String TOPICS_DOC = "";
    public static final String TOPICS_DEFAULT = "";
    private static final String TOPICS_DISPLAY = "Topics";

    static ConfigDef config = ConnectorConfig.configDef()
        .define(TOPICS_CONFIG, ConfigDef.Type.LIST, TOPICS_DEFAULT, ConfigDef.Importance.HIGH, TOPICS_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_DISPLAY);

    public SinkConnectorConfig() {
        this(new HashMap<String, String>());
    }

    public static ConfigDef configDef() {
        return config;
    }

    public SinkConnectorConfig(Map<String, String> props) {
        super(config, props);
    }
}
