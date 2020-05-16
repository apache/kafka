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
package org.apache.kafka.common.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A implementation of MetricsContext, it encapsulates required metrics context properties for Kafka services and clients
 */
public class KafkaMetricsContext implements MetricsContext {
    /**
     * Client or Service's metadata map.
     */
    private Map<String, String> metadata = new HashMap<>();

    /**
     * Create a MetricsContext with namespace, no service or client properties
     * @param namespace value for _namespace key
     */
    public KafkaMetricsContext(String namespace) {
        this(namespace, new HashMap<>());
    }

    /**
     * Create a MetricsContext with namespace, service or client properties
     * @param namespace value for _namespace key
     * @param metadata  metadata additional entries to add to the context
     *                  values will be converted to string using Object.toString()
     */
    public KafkaMetricsContext(String namespace, Map<String, ?> metadata) {
        this.metadata.put(MetricsContext.NAMESPACE, namespace);
        this.metadata.putAll(metadata.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey(),
            entry -> entry.getValue().toString()))
        );
    }

    public Map<String, String> metadata() {
        return Collections.unmodifiableMap(metadata);
    }

}
