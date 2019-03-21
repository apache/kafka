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

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A new topic to be created via {@link AdminClient#createTopics(Collection)}.
 */
public class NewTopic {
    private final String name;
    private final int numPartitions;
    private final short replicationFactor;
    private final Map<Integer, List<Integer>> replicasAssignments;
    private Map<String, String> configs = null;

    /**
     * A new topic with the specified replication factor and number of partitions.
     */
    public NewTopic(String name, int numPartitions, short replicationFactor) {
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
        this.numPartitions = -1;
        this.replicationFactor = -1;
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
        return numPartitions;
    }

    /**
     * The replication factor for the new topic or -1 if a replica assignment has been specified.
     */
    public short replicationFactor() {
        return replicationFactor;
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
            setNumPartitions(numPartitions).
            setReplicationFactor(replicationFactor);
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
                append(", numPartitions=").append(numPartitions).
                append(", replicationFactor=").append(replicationFactor).
                append(", replicasAssignments=").append(replicasAssignments).
                append(", configs=").append(configs).
                append(")");
        return bld.toString();
    }
}
