/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Represents a user defined endpoint in a {@link org.apache.kafka.streams.KafkaStreams} application.
 * Instances of this class can be obtained by calling one of:
 *  {@link KafkaStreams#allMetadata()}
 *  {@link KafkaStreams#allMetadataForStore(String)}
 *  {@link KafkaStreams#metadataForKey(String, Object, StreamPartitioner)}
 *  {@link KafkaStreams#metadataForKey(String, Object, Serializer)}
 *
 *  The HostInfo is constructed during Partition Assignment
 *  see {@link org.apache.kafka.streams.processor.internals.StreamPartitionAssignor}
 *  It is extracted from the config {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG}
 *
 *  If developers wish to expose an endpoint in their KafkaStreams applications they should provide the above
 *  config.
 */
public class HostInfo {
    private final String host;
    private final int port;

    public HostInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostInfo hostInfo = (HostInfo) o;

        if (port != hostInfo.port) return false;
        return host.equals(hostInfo.host);

    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public String toString() {
        return "HostInfo{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
