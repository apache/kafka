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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Configuration options for Connectors. These only include Kafka Connect system-level configuration
 * options (e.g. Connector class name, timeouts used by Connect to control the connector) but does
 * not include Connector-specific options (e.g. database connection settings).
 * </p>
 * <p>
 * Note that some of these options are not required for all connectors. For example TOPICS_CONFIG
 * is sink-specific.
 * </p>
 */
public class ConnectorConfig extends AbstractConfig {

    public static final String NAME_CONFIG = "name";
    private static final String NAME_DOC = "Globally unique name to use for this connector.";

    public static final String CONNECTOR_CLASS_CONFIG = "connector.class";
    private static final String CONNECTOR_CLASS_DOC =
            "Name of the class for this connector. Must be a subclass of org.apache.kafka.connect.connector.Connector";

    public static final String TASKS_MAX_CONFIG = "tasks.max";
    private static final String TASKS_MAX_DOC = "Maximum number of tasks to use for this connector.";
    public static final int TASKS_MAX_DEFAULT = 1;

    public static final String TOPICS_CONFIG = "topics";
    private static final String TOPICS_DOC = "";
    public static final String TOPICS_DEFAULT = "";

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(NAME_CONFIG, Type.STRING, Importance.HIGH, NAME_DOC)
                .define(CONNECTOR_CLASS_CONFIG, Type.CLASS, Importance.HIGH, CONNECTOR_CLASS_DOC)
                .define(TASKS_MAX_CONFIG, Type.INT, TASKS_MAX_DEFAULT, Importance.HIGH, TASKS_MAX_DOC)
                .define(TOPICS_CONFIG, Type.LIST, TOPICS_DEFAULT, Importance.HIGH, TOPICS_DOC);
    }

    public ConnectorConfig() {
        this(new HashMap<String, String>());
    }

    public ConnectorConfig(Map<String, String> props) {
        super(config, props);
    }
}
