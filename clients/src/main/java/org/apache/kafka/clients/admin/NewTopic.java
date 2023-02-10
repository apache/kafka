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

package org.apache.kafka.clients.admin;

import java.util.Optional;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.requests.CreateTopicsRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

/**
 * A new topic to be created via {@link Admin#createTopics(Collection)}.
 */
public class NewTopic {

    private final String name;
    private final Optional<Integer> numPartitions;
    private final Optional<Short> replicationFactor;
    private final Map<Integer, List<Integer>> replicasAssignments;
    private Map<String, String> configs = null;

    /**
     * A new topic with the specified replication factor and number of partitions.
     */
    public NewTopic(String name, int numPartitions, short replicationFactor) {
        this(name, Optional.of(numPartitions), Optional.of(replicationFactor));
    }

    /**
     * A new topic that optionally defaults {@code numPartitions} and {@code replicationFactor} to
     * the broker configurations for {@code num.partitions} and {@code default.replication.factor}
     * respectively.
     */
    public NewTopic(String name, Optional<Integer> numPartitions, Optional<Short> replicationFactor) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.replicasAssignments = null;
    }

    /**
     * A new topic with the specified replica assignment configuration.
     *
     * @param name the topic name.
     * @param replicasAssignments a map from partition id to replica ids (i.e. broker ids). Although not enforced, it is
     *                            generally a good idea for all partitions to have the same number of replicas.
     */
    public NewTopic(String name, Map<Integer, List<Integer>> replicasAssignments) {
        this.name = name;
        this.numPartitions = Optional.empty();
        this.replicationFactor = Optional.empty();
        this.replicasAssignments = Collections.unmodifiableMap(replicasAssignments);
    }

    /**
     * The name of the topic to be created.
     */
    public String name() {
        return name;
    }

    /**
     * The number of partitions for the new topic or -1 if a replica assignment has been specified.
     */
    public int numPartitions() {
        return numPartitions.orElse(CreateTopicsRequest.NO_NUM_PARTITIONS);
    }

    /**
     * The replication factor for the new topic or -1 if a replica assignment has been specified.
     */
    public short replicationFactor() {
        return replicationFactor.orElse(CreateTopicsRequest.NO_REPLICATION_FACTOR);
    }

    /**
     * A map from partition id to replica ids (i.e. broker ids) or null if the number of partitions and replication
     * factor have been specified instead.
     */
    public Map<Integer, List<Integer>> replicasAssignments() {
        return replicasAssignments;
    }

    /**
     * Set the configuration to use on the new topic.
     *
     * @param configs               The configuration map.
     * @return                      This NewTopic object.
     */
    public NewTopic configs(Map<String, String> configs) {
        this.configs = configs;
        return this;
    }

    /**
     * The configuration for the new topic or null if no configs ever specified.
     */
    public Map<String, String> configs() {
        return configs;
    }

    CreatableTopic convertToCreatableTopic() {
        CreatableTopic creatableTopic = new CreatableTopic().
            setName(name).
            setNumPartitions(numPartitions.orElse(CreateTopicsRequest.NO_NUM_PARTITIONS)).
            setReplicationFactor(replicationFactor.orElse(CreateTopicsRequest.NO_REPLICATION_FACTOR));
        if (replicasAssignments != null) {
            for (Entry<Integer, List<Integer>> entry : replicasAssignments.entrySet()) {
                creatableTopic.assignments().add(
                    new CreatableReplicaAssignment().
                        setPartitionIndex(entry.getKey()).
                        setBrokerIds(entry.getValue()));
            }
        }
        if (configs != null) {
            for (Entry<String, String> entry : configs.entrySet()) {
                creatableTopic.configs().add(
                    new CreateableTopicConfig().
                        setName(entry.getKey()).
                        setValue(entry.getValue()));
            }
        }
        return creatableTopic;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(name=").append(name).
                append(", numPartitions=").append(numPartitions.map(String::valueOf).orElse("default")).
                append(", replicationFactor=").append(replicationFactor.map(String::valueOf).orElse("default")).
                append(", replicasAssignments=").append(replicasAssignments).
                append(", configs=").append(configs).
                append(")");
        return bld.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final NewTopic that = (NewTopic) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(numPartitions, that.numPartitions) &&
            Objects.equals(replicationFactor, that.replicationFactor) &&
            Objects.equals(replicasAssignments, that.replicasAssignments) &&
            Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, numPartitions, replicationFactor, replicasAssignments, configs);
    }
}
