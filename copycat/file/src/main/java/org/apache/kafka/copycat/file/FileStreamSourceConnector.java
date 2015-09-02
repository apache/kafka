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

package org.apache.kafka.copycat.file;

import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class FileStreamSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";

    private String filename;
    private String topic;

    @Override
    public void start(Properties props) {
        filename = props.getProperty(FILE_CONFIG);
        topic = props.getProperty(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new CopycatException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new CopycatException("FileStreamSourceConnector should only have a single topic when used as a source.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Properties> taskConfigs(int maxTasks) {
        ArrayList<Properties> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Properties config = new Properties();
        if (filename != null)
            config.setProperty(FILE_CONFIG, filename);
        config.setProperty(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
    }
}
