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
 * Configuration options for Tasks. These only include Kafka Connect system-level configuration
 * options.
 * </p>
 */
public class TaskConfig extends AbstractConfig {

    public static final String TASK_CLASS_CONFIG = "task.class";
    private static final String TASK_CLASS_DOC =
            "Name of the class for this task. Must be a subclass of org.apache.kafka.connect.connector.Task";

    private static ConfigDef config;

    static {
        config = new ConfigDef()
                .define(TASK_CLASS_CONFIG, Type.CLASS, Importance.HIGH, TASK_CLASS_DOC);
    }

    public TaskConfig() {
        this(new HashMap<String, String>());
    }

    public TaskConfig(Map<String, ?> props) {
        super(config, props);
    }
}
