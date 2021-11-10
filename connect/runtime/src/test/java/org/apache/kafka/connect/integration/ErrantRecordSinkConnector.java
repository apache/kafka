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

package org.apache.kafka.connect.integration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ErrantRecordSinkConnector extends MonitorableSinkConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return ErrantRecordSinkTask.class;
    }

    public static class ErrantRecordSinkTask extends MonitorableSinkTask {
        private ErrantRecordReporter reporter;

        public ErrantRecordSinkTask() {
            super();
        }

        @Override
        public void start(Map<String, String> props) {
            super.start(props);
            reporter = context.errantRecordReporter();
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord rec : records) {
                taskHandle.record();
                TopicPartition tp = cachedTopicPartitions
                    .computeIfAbsent(rec.topic(), v -> new HashMap<>())
                    .computeIfAbsent(rec.kafkaPartition(), v -> new TopicPartition(rec.topic(), rec.kafkaPartition()));
                committedOffsets.put(tp, committedOffsets.getOrDefault(tp, 0) + 1);
                reporter.report(rec, new Throwable());
            }
        }
    }
}
