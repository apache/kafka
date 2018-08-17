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
package org.apache.kafka.connect.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.TestSinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorableSinkConnector extends TestSinkConnector {

    private static final Logger log = LoggerFactory.getLogger(MonitorableSinkConnector.class);

    public static AtomicInteger COUNTER = new AtomicInteger();

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SimpleTestSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("required", "dummy-val");
            configs.add(config);
        }
        return configs;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    public static class SimpleTestSinkTask extends SinkTask {

        @Override
        public String version() {
            return null;
        }

        @Override
        public void start(Map<String, String> props) {
            log.debug("Starting task {}", context);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord rec : records) {
                COUNTER.incrementAndGet();
                log.debug("Obtained record: {} at {}", rec.value(), context);
            }
        }

        @Override
        public void stop() {
        }

    }

}
