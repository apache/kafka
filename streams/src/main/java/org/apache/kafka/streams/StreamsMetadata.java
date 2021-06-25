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
package org.apache.kafka.streams;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Set;

/**
 * Represents the state of the different a given Kafka Streams instance running within a {@link KafkaStreams} application.
 */
public interface StreamsMetadata {

    /**
     * The value of {@link StreamsConfig#APPLICATION_SERVER_CONFIG} configured for the streams
     * instance, which is typically host/port
     *
     * @return {@link HostInfo} corresponding to the streams instance
     */
    HostInfo hostInfo();

    /**
     * State stores owned by the instance as an active replica
     *
     * @return set of active state store names
     */
    Set<String> stateStoreNames();

    /**
     * Topic partitions consumed by the instance as an active replica
     *
     * @return set of active topic partitions
     */
    Set<TopicPartition> topicPartitions();

    /**
     * (Source) Topic partitions for which the instance acts as standby.
     *
     * @return set of standby topic partitions
     */
    Set<TopicPartition> standbyTopicPartitions();

    /**
     * State stores owned by the instance as a standby replica
     *
     * @return set of standby state store names
     */
    Set<String> standbyStateStoreNames();

    /**
     * This method is equivalent to call {@code StreamsMetadata.hostInfo().host();}
     *
     * @return the host where the given process runs
     */
    String host();

    /**
     * This method is equivalent to call {@code StreamsMetadata.hostInfo().port();}
     *
     * @return the port number where the given process runs
     */
    int port();

    /**
     * Compares the specified object with this StreamsMetadata. Returns {@code true} if and only if the specified object is
     * also a StreamsMetadata and for both {@code hostInfo()} are equal, and {@code stateStoreNames()}, {@code topicPartitions()},
     * {@code standbyStateStoreNames()}, and {@code standbyTopicPartitions()} contain the same elements.
     *
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     */
    boolean equals(Object o);

    /**
     * Returns the hash code value for this TaskMetadata. The hash code of a list is defined to be the result of the following calculation:
     * <pre>
     * {@code
     * Objects.hash(hostInfo(), stateStoreNames(), topicPartitions(), standbyStateStoreNames(), standbyTopicPartitions());
     * }
     * </pre>
     *
     * @return a hash code value for this object.
     */
    int hashCode();

}
