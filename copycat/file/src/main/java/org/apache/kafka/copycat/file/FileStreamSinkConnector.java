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
import org.apache.kafka.copycat.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class FileStreamSinkConnector extends SinkConnector {
    public static final String FILE_CONFIG = "file";

    private String filename;

    @Override
    public void start(Properties props) {
        filename = props.getProperty(FILE_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSinkTask.class;
    }

    @Override
    public List<Properties> taskConfigs(int maxTasks) {
        ArrayList<Properties> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Properties config = new Properties();
            if (filename != null)
                config.setProperty(FILE_CONFIG, filename);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }
}
