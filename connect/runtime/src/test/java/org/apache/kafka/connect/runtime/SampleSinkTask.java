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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SampleSinkTask extends SinkTask {

    private Set<TopicPartition> currentAssignment;
    @Override
    public String version() {
        return null;
    }

    public void open(Collection<TopicPartition> topicPartitions) {
        this.currentAssignment = context.assignment();
    }

    public void close(Collection<TopicPartition> topicPartitions) {
        this.currentAssignment = context.assignment();
    }

    @Override
    public void start(Map<String, String> props) {
        this.currentAssignment = new HashSet<>();
    }

    @Override
    public void put(Collection<SinkRecord> records) {

    }

    public Set<TopicPartition> getCurrentAssignment() {
        return this.currentAssignment;
    }

    @Override
    public void stop() {

    }
}
