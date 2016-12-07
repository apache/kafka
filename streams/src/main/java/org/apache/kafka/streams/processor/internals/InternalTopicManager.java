/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InternalTopicManager {

    private static final Logger log = LoggerFactory.getLogger(InternalTopicManager.class);

    // TODO: the following ZK dependency should be removed after KIP-4
    private static final String ZK_TOPIC_PATH = "/brokers/topics";
    private static final String ZK_BROKER_PATH = "/brokers/ids";
    private static final String ZK_DELETE_TOPIC_PATH = "/admin/delete_topics";
    private static final String ZK_ENTITY_CONFIG_PATH = "/config/topics";
    // TODO: the following LogConfig dependency should be removed after KIP-4
    public static final String CLEANUP_POLICY_PROP = "cleanup.policy";
    public static final String RETENTION_MS = "retention.ms";
    public static final Long WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    private final ZkClient zkClient;
    private final int replicationFactor;
    private final long windowChangeLogAdditionalRetention;

    private class ZKStringSerializer implements ZkSerializer {

        /**
         * @throws AssertionError if the byte String encoding type is not supported
         */
        @Override
        public byte[] serialize(Object data) {
            try {
                return ((String) data).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new AssertionError(e);
            }
        }

        /**
         * @throws AssertionError if the byte String encoding type is not supported
         */
        @Override
        public Object deserialize(byte[] bytes) {
            try {
                if (bytes == null)
                    return null;
                else
                    return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new AssertionError(e);
            }
        }
    }

    public InternalTopicManager() {
        this.zkClient = null;
        this.replicationFactor = 0;
        this.windowChangeLogAdditionalRetention = WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;
    }

    public InternalTopicManager(String zkConnect, final int replicationFactor, long windowChangeLogAdditionalRetention) {
        this.zkClient = new ZkClient(zkConnect, 30 * 1000, 30 * 1000, new ZKStringSerializer());
        this.replicationFactor = replicationFactor;
        this.windowChangeLogAdditionalRetention = windowChangeLogAdditionalRetention;
    }

    public void makeReady(InternalTopicConfig topic, int numPartitions) {
        boolean topicNotReady = true;

        while (topicNotReady) {
            Map<Integer, List<Integer>> topicMetadata = getTopicMetadata(topic.name());

            if (topicMetadata == null) {
                try {
                    createTopic(topic, numPartitions, replicationFactor);
                } catch (ZkNodeExistsException e) {
                    // ignore and continue
                }
            } else {
                if (topicMetadata.size() > numPartitions) {
                    // else if topic exists with more #.partitions than needed, delete in order to re-create it
                    try {
                        deleteTopic(topic.name());
                    } catch (ZkNodeExistsException e) {
                        // ignore and continue
                    }
                } else if (topicMetadata.size() < numPartitions) {
                    // else if topic exists with less #.partitions than needed, add partitions
                    try {
                        addPartitions(topic.name(), numPartitions - topicMetadata.size(), replicationFactor, topicMetadata);
                    } catch (ZkNoNodeException e) {
                        // ignore and continue
                    }
                } else {
                    topicNotReady = false;
                }
            }
        }
    }

    private List<Integer> getBrokers() {
        List<Integer> brokers = new ArrayList<>();
        for (String broker: zkClient.getChildren(ZK_BROKER_PATH)) {
            brokers.add(Integer.parseInt(broker));
        }
        Collections.sort(brokers);

        log.debug("Read brokers {} from ZK in partition assignor.", brokers);

        return brokers;
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, List<Integer>> getTopicMetadata(String topic) {
        String data = zkClient.readData(ZK_TOPIC_PATH + "/" + topic, true);

        if (data == null) return null;

        try {
            ObjectMapper mapper = new ObjectMapper();

            Map<String, Object> dataMap = mapper.readValue(data, new TypeReference<Map<String, Object>>() {

            });

            Map<Integer, List<Integer>> partitions = (Map<Integer, List<Integer>>) dataMap.get("partitions");

            log.debug("Read partitions {} for topic {} from ZK in partition assignor.", partitions, topic);

            return partitions;
        } catch (IOException e) {
            throw new StreamsException("Error while reading topic metadata from ZK for internal topic " + topic, e);
        }
    }

    private void createTopic(InternalTopicConfig topic, int numPartitions, int replicationFactor) throws ZkNodeExistsException {
        log.debug("Creating topic {} with {} partitions from ZK in partition assignor.", topic.name(), numPartitions);
        ObjectMapper mapper = new ObjectMapper();
        List<Integer> brokers = getBrokers();
        int numBrokers = brokers.size();
        if (numBrokers < replicationFactor) {
            log.warn("Not enough brokers found. The replication factor is reduced from " + replicationFactor + " to " +  numBrokers);
            replicationFactor = numBrokers;
        }

        Map<Integer, List<Integer>> assignment = new HashMap<>();

        for (int i = 0; i < numPartitions; i++) {
            ArrayList<Integer> brokerList = new ArrayList<>();
            for (int r = 0; r < replicationFactor; r++) {
                int shift = r * numBrokers / replicationFactor;
                brokerList.add(brokers.get((i + shift) % numBrokers));
            }
            assignment.put(i, brokerList);
        }
        // write out config first just like in AdminUtils.scala createOrUpdateTopicPartitionAssignmentPathInZK()
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("version", 1);
            dataMap.put("config", topic.toProperties(windowChangeLogAdditionalRetention));
            String data = mapper.writeValueAsString(dataMap);
            zkClient.createPersistent(ZK_ENTITY_CONFIG_PATH + "/" + topic.name(), data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (JsonProcessingException e) {
            throw new StreamsException("Error while creating topic config in ZK for internal topic " + topic, e);
        }

        // try to write to ZK with open ACL
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("version", 1);
            dataMap.put("partitions", assignment);
            String data = mapper.writeValueAsString(dataMap);

            zkClient.createPersistent(ZK_TOPIC_PATH + "/" + topic.name(), data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (JsonProcessingException e) {
            throw new StreamsException("Error while creating topic metadata in ZK for internal topic " + topic, e);
        }
    }

    private void deleteTopic(String topic) throws ZkNodeExistsException {
        log.debug("Deleting topic {} from ZK in partition assignor.", topic);

        zkClient.createPersistent(ZK_DELETE_TOPIC_PATH + "/" + topic, "", ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    private void addPartitions(String topic, int numPartitions, int replicationFactor, Map<Integer, List<Integer>> existingAssignment) {
        log.debug("Adding {} partitions topic {} from ZK with existing partitions assigned as {} in partition assignor.", topic, numPartitions, existingAssignment);

        List<Integer> brokers = getBrokers();
        int numBrokers = brokers.size();
        if (numBrokers < replicationFactor) {
            log.warn("Not enough brokers found. The replication factor is reduced from " + replicationFactor + " to " +  numBrokers);
            replicationFactor = numBrokers;
        }

        int startIndex = existingAssignment.size();

        Map<Integer, List<Integer>> newAssignment = new HashMap<>(existingAssignment);

        for (int i = 0; i < numPartitions; i++) {
            ArrayList<Integer> brokerList = new ArrayList<>();
            for (int r = 0; r < replicationFactor; r++) {
                int shift = r * numBrokers / replicationFactor;
                brokerList.add(brokers.get((i + shift) % numBrokers));
            }
            newAssignment.put(i + startIndex, brokerList);
        }

        // try to write to ZK with open ACL
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("version", 1);
            dataMap.put("partitions", newAssignment);

            ObjectMapper mapper = new ObjectMapper();
            String data = mapper.writeValueAsString(dataMap);

            zkClient.writeData(ZK_TOPIC_PATH + "/" + topic, data);
        } catch (JsonProcessingException e) {
            throw new StreamsException("Error while updating topic metadata in ZK for internal topic " + topic, e);
        }
    }

}
