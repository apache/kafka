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
package org.apache.kafka.testkit;

import org.apache.kafka.common.utils.LogContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds a new test cluster.
 */
public class MiniKafkaClusterBuilder {
    LogContext logContext = new LogContext("");

    Map<String, String> configs = new HashMap<>();

    Collection<MiniKafkaNodeBuilder> kafkaBlds = new ArrayList<>();

    Collection<MiniZookeeperNodeBuilder> zkBlds = new ArrayList<>(3);

    public MiniKafkaClusterBuilder logContext(LogContext logContext) {
        this.logContext = logContext;
        return this;
    }

    public MiniKafkaClusterBuilder config(String key, String value) {
        this.configs.put(key, value);
        return this;
    }

    public MiniKafkaClusterBuilder configs(Map<String, String> configs) {
        this.configs.putAll(configs);
        return this;
    }

    public MiniKafkaClusterBuilder addNode(MiniKafkaNodeBuilder kafkaBld) {
        kafkaBlds.add(kafkaBld);
        return this;
    }

    public MiniKafkaClusterBuilder addZookeeperNode(MiniZookeeperNodeBuilder zkBld) {
        zkBlds.add(zkBld);
        return this;
    }

    public MiniKafkaCluster build() {
        return new MiniKafkaCluster(this);
    }
}
