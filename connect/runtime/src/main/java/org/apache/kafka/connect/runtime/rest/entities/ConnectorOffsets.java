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
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a list of {partition, offset} pairs (each pair is a {@link ConnectorOffset}), used as request or response
 * bodies for offset management REST APIs. The partitions and offsets could be for either a sink connector or a source
 * connector. For source connectors, the partition and offset structures are defined by the connector implementations
 * themselves. For a sink connector, where offsets represent the underlying Kafka consumer group offsets, this would
 * look something like:
 * <pre>
 *     {
 *       "offsets": [
 *         {
 *           "partition": {
 *             "kafka_topic": "topic"
 *             "kafka_partition": 3
 *           },
 *           "offset": {
 *             "kafka_offset": 1000
 *           }
 *         }
 *       ]
 *     }
 * </pre>
 *
 * @see ConnectorsResource#getOffsets
 * @see ConnectorsResource#alterConnectorOffsets
 */
public class ConnectorOffsets {
    private final List<ConnectorOffset> offsets;

    @JsonCreator
    public ConnectorOffsets(@JsonProperty("offsets") List<ConnectorOffset> offsets) {
        this.offsets = offsets;
    }

    @JsonProperty
    public List<ConnectorOffset> offsets() {
        return offsets;
    }

    public Map<Map<String, ?>, Map<String, ?>> toMap() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsetMap = new HashMap<>();
        for (ConnectorOffset offset : offsets) {
            partitionOffsetMap.put(offset.partition(), offset.offset());
        }
        return partitionOffsetMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ConnectorOffsets)) {
            return false;
        }
        ConnectorOffsets that = (ConnectorOffsets) obj;
        return Objects.equals(this.offsets, that.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offsets);
    }
}
