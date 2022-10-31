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
package org.apache.kafka.streams.state;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;

/**
 * Represents a user defined endpoint in a {@link org.apache.kafka.streams.KafkaStreams} application.
 * Instances of this class can be obtained by calling one of:
 *  {@link KafkaStreams#metadataForAllStreamsClients()}
 *  {@link KafkaStreams#streamsMetadataForStore(String)}
 *
 *  The HostInfo is constructed during Partition Assignment
 *  see {@link StreamsPartitionAssignor}
 *  It is extracted from the config {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG}
 *
 *  If developers wish to expose an endpoint in their KafkaStreams applications they should provide the above
 *  config.
 */
public class HostInfo {
    private final String host;
    private final int port;

    public HostInfo(final String host,
                    final int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * @throws ConfigException if the host or port cannot be parsed from the given endpoint string
     * @return a new HostInfo or null if endPoint is null or has no characters
     */
    public static HostInfo buildFromEndpoint(final String endPoint) {
        if (Utils.isBlank(endPoint)) {
            return null;
        }

        final String host = getHost(endPoint);
        final Integer port = getPort(endPoint);

        if (host == null || port == null) {
            throw new ConfigException(
                String.format("Error parsing host address %s. Expected format host:port.", endPoint)
            );
        }
        return new HostInfo(host, port);
    }

    /**
     * @return a sentinel for cases where the host metadata is currently unavailable, eg during rebalance operations.
     */
    public static HostInfo unavailable() {
        return new HostInfo("unavailable", -1);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final HostInfo hostInfo = (HostInfo) o;
        return port == hostInfo.port && host.equals(hostInfo.host);
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
                "host=\'" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
