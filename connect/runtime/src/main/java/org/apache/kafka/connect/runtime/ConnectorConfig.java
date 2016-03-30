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
import org.apache.kafka.common.config.ConfigDef.Width;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


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
    private static final String COMMON_GROUP = "Common";

    public static final String NAME_CONFIG = "name";
    private static final String NAME_DOC = "Globally unique name to use for this connector.";
    private static final String NAME_DISPLAY = "Connector name";

    public static final String CONNECTOR_CLASS_CONFIG = "connector.class";
    private static final String CONNECTOR_CLASS_DOC =
                    "Name or alias of the class for this connector. Must be a subclass of org.apache.kafka.connect.connector.Connector. " +
                    "If the connector is org.apache.kafka.connect.file.FileStreamSinkConnector, you can either specify this full name, " +
                    " or use \"FileStreamSink\" or \"FileStreamSinkConnector\" to make the configuration a bit shorter";
    private static final String CONNECTOR_CLASS_DISPLAY = "Connector class";

    public static final String TASKS_MAX_CONFIG = "tasks.max";
    private static final String TASKS_MAX_DOC = "Maximum number of tasks to use for this connector.";
    public static final int TASKS_MAX_DEFAULT = 1;
    private static final int TASKS_MIN_CONFIG = 1;

    private static final String TASK_MAX_DISPLAY = "Tasks max";

    public static final String TOPICS_CONFIG = "topics";
    private static final String TOPICS_DOC = "";
    public static final String TOPICS_DEFAULT = "";
    private static final String TOPICS_DISPLAY = "Topics";

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(NAME_CONFIG, Type.STRING, Importance.HIGH, NAME_DOC, COMMON_GROUP, 1, Width.MEDIUM, NAME_DISPLAY)
                .define(CONNECTOR_CLASS_CONFIG, Type.STRING, Importance.HIGH, CONNECTOR_CLASS_DOC, COMMON_GROUP, 2, Width.LONG, CONNECTOR_CLASS_DISPLAY)
                .define(TASKS_MAX_CONFIG, Type.INT, TASKS_MAX_DEFAULT,  atLeast(TASKS_MIN_CONFIG), Importance.HIGH, TASKS_MAX_DOC, COMMON_GROUP, 3, Width.SHORT, TASK_MAX_DISPLAY)
                .define(TOPICS_CONFIG, Type.LIST, TOPICS_DEFAULT, Importance.HIGH, TOPICS_DOC, COMMON_GROUP, 4, Width.LONG, TOPICS_DISPLAY);
    }

    public static ConfigDef configDef() {
        return config;
    }

    public ConnectorConfig() {
        this(new HashMap<String, String>());
    }

    public ConnectorConfig(Map<String, String> props) {
        super(config, props);
    }
}
