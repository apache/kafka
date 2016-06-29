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
 *  {@link KafkaStreams#getAllTasks()}
 *  {@link KafkaStreams#getAllTasksWithStore(String)}
 *  {@link KafkaStreams#getTaskWithKey(String, Object, StreamPartitioner)}
 *  {@link KafkaStreams#getTaskWithKey(String, Object, Serializer)}
 *
 *  The HostState is constructed during Partition Assignment
 *  see {@link org.apache.kafka.streams.processor.internals.StreamPartitionAssignor}
 *  It is extracted from the config {@link org.apache.kafka.streams.StreamsConfig#USER_ENDPOINT_CONFIG}
 *
 *  If developers wish to expose an endpoint in their KafkaStreams applications they should provide the above
 *  config.
 */
public class HostState {
    private final String host;
    private final int port;

    public HostState(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostState hostState = (HostState) o;

        if (port != hostState.port) return false;
        return host.equals(hostState.host);

    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "HostState{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
