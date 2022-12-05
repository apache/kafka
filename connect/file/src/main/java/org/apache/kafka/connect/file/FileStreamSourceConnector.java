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
package org.apache.kafka.connect.file;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Very simple source connector that works with stdin or a file.
 */
public class FileStreamSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceConnector.class);
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FILE_CONFIG, Type.STRING, null, Importance.HIGH, "Source filename. If not specified, the standard input will be used")
        .define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to")
        .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
                "The maximum number of records the source task can read from the file each time it is polled");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String filename = config.getString(FILE_CONFIG);
        filename = (filename == null || filename.isEmpty()) ? "standard input" : filename;
        log.info("Starting file source connector reading from {}", filename);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        String filename = parsedConfig.getString(FILE_CONFIG);
        // We can provide exactly-once guarantees if reading from a "real" file
        // (as long as the file is only appended to over the lifetime of the connector)
        // If we're reading from stdin, we can't provide exactly-once guarantees
        // since we don't even track offsets
        return filename != null && !filename.isEmpty()
                ? ExactlyOnceSupport.SUPPORTED
                : ExactlyOnceSupport.UNSUPPORTED;
    }

}
