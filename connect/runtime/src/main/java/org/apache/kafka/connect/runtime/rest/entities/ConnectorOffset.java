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
package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a single {partition, offset} pair for either a sink connector or a source connector. For source connectors,
 * the partition and offset structures are defined by the connector implementations themselves. For a sink connector,
 * where offsets represent the underlying Kafka consumer group offsets, this would look something like:
 * <pre>
 *     {
 *       "partition": {
 *         "kafka_topic": "topic"
 *         "kafka_partition": 3
 *       },
 *       "offset": {
 *         "kafka_offset": 1000
 *       }
 *     }
 * </pre>
 */
public class ConnectorOffset {

    private final Map<String, ?> partition;
    private final Map<String, ?> offset;

    @JsonCreator
    public ConnectorOffset(@JsonProperty("partition") Map<String, ?> partition, @JsonProperty("offset") Map<String, ?> offset) {
        this.partition = partition;
        this.offset = offset;
    }

    @JsonProperty
    public Map<String, ?> partition() {
        return partition;
    }

    @JsonProperty
    public Map<String, ?> offset() {
        return offset;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ConnectorOffset)) {
            return false;
        }
        ConnectorOffset that = (ConnectorOffset) obj;
        return Objects.equals(this.partition, that.partition) &&
                Objects.equals(this.offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, offset);
    }
}
