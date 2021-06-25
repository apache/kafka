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
 * Metadata of a Kafka Streams client.
 */
public interface StreamsMetadata {

    /**
     * The value of {@link StreamsConfig#APPLICATION_SERVER_CONFIG} configured for the Streams
     * client.
     *
     * @return {@link HostInfo} corresponding to the Streams client
     */
    HostInfo hostInfo();

    /**
     * Names of the state stores assigned to active tasks of the Streams client.
     *
     * @return names of the state stores assigned to active tasks
     */
    Set<String> stateStoreNames();

    /**
     * Source topic partitions of the active tasks of the Streams client.
     *
     * @return source topic partitions of the active tasks
     */
    Set<TopicPartition> topicPartitions();

    /**
     * Changelog topic partitions for the state stores the standby tasks of the Streams client replicates.
     *
     * @return set of changelog topic partitions of the standby tasks
     */
    Set<TopicPartition> standbyTopicPartitions();

    /**
     * Names of the state stores assigned to standby tasks of the Streams client.
     *
     * @return names of the state stores assigned to standby tasks
     */
    Set<String> standbyStateStoreNames();

    /**
     * Host where the Streams client runs. 
     *
     * This method is equivalent to {@code StreamsMetadata.hostInfo().host();}
     *
     * @return the host where the Streams client runs
     */
    String host();

    /**
     * Port on which the Streams client listens.
     * 
     * This method is equivalent to {@code StreamsMetadata.hostInfo().port();}
     *
     * @return the port on which Streams client listens
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
