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

import java.util.HashMap;
import java.util.Map;

/**
 * A implementation of MetricsContext, it encapsulates required metrics context properties for Kafka
 */
public class KafkaMetricsContext implements MetricsContext {
    public static final String METRICS_CONTEXT_PREFIX = "metrics.context.";

    /**
     * Client or Service's metadata map.
     */
    private Map<String, String> metadata = new HashMap<>();

    public KafkaMetricsContext() {
    }

    public KafkaMetricsContext(String namespace) {
        metadata.put(MetricsContext.NAMESPACE, namespace);
    }

    /**
     *
     * @param namespace value for _namespace key
     * @param properties key/value pairs passed in from client/service properties file
     */
    public KafkaMetricsContext(String namespace, Map<String, Object> properties) {
        metadata.put(MetricsContext.NAMESPACE, namespace);
        //add properties start with metrics.context from properties file
        for (String key : properties.keySet()) {
            if (key.startsWith(METRICS_CONTEXT_PREFIX)) {
                metadata.put(key.substring(METRICS_CONTEXT_PREFIX.length()), (String) properties.get(key));
            }
        }
    }

    /**
     * Returns client's metadata map.
     *
     * @return metadata fields
     */
    public Map<String, String> metadata() {
        return metadata;
    }

}
