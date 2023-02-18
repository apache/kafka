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

package org.apache.kafka.connect.mirror.clients.admin;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/** FakeLocalMetadataStore is used for MM2's integration test.
 * The class store metadata of all topics/ACLs created or altered cross clusters using MM2 integration test.
 * */
public class FakeLocalMetadataStore {
    private static final Logger log = LoggerFactory.getLogger(FakeLocalMetadataStore.class);

    private static ConcurrentHashMap<String, ConcurrentHashMap<String, String>> allTopics = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Vector<AclBinding>> allAcls = new ConcurrentHashMap<>();

    /**
     * Add topic to allTopics.
     * @param newTopic {@link NewTopic}
     */
    public static void addTopicToLocalMetadataStore(NewTopic newTopic) {
        ConcurrentHashMap<String, String> configs = new ConcurrentHashMap<>(newTopic.configs());
        configs.putIfAbsent("partitions", String.valueOf(newTopic.numPartitions()));
        allTopics.putIfAbsent(newTopic.name(), configs);
    }

    /**
     * update partition count for given topic
     * @param topic topic name
     * @param newPartitionCount new partition count.
     */
    public static void updatePartitionCount(String topic, int newPartitionCount) {
        ConcurrentHashMap<String, String> configs = FakeLocalMetadataStore.allTopics.getOrDefault(topic, new ConcurrentHashMap<>());
        configs.compute("partitions", (key, value) -> String.valueOf(newPartitionCount));
        FakeLocalMetadataStore.allTopics.putIfAbsent(topic, configs);
    }

    /**
     * update topic configuration.
     * @param topic topic name
     * @param newConfig topic config
     */
    public static void updateTopicConfig(String topic, Config newConfig) {
        ConcurrentHashMap<String, String> topicConfigs = FakeLocalMetadataStore.allTopics.getOrDefault(topic, new ConcurrentHashMap<>());
        newConfig.entries().stream().forEach(configEntry -> {
            if (configEntry.name() != null) {
                if (configEntry.value() != null) {
                    log.debug("Topic '{}' update config '{}' to '{}'", topic, configEntry.name(), configEntry.value());
                    topicConfigs.compute(configEntry.name(), (key, value) -> configEntry.value());
                } else {
                    log.warn("Topic '{}' has config '{}' set to null", topic, configEntry.name());
                }
            }
        });
        FakeLocalMetadataStore.allTopics.putIfAbsent(topic, topicConfigs);
    }

    /**
     * check if allTopics contains topic name.
     * @param topic name of topic
     * @return true if topic name is a key in allTopics
     */
    public static Boolean containsTopic(String topic) {
        return allTopics.containsKey(topic);
    }

    /**
     * get topic config stored in allTopics.
     * @param topic name of topic
     * @return topic configurations.
     */
    public static Map<String, String> topicConfig(String topic) {
        return allTopics.getOrDefault(topic, new ConcurrentHashMap<>());
    }

    /**
     * get list of {@link AclBinding} stored for kafka principle in allACLs.
     * @param aclPrinciple name of kafka user
     * @return {@link List<AclBinding>}
     */
    public static List<AclBinding> aclBindings(String aclPrinciple) {
        return FakeLocalMetadataStore.allAcls.getOrDefault("User:" + aclPrinciple, new Vector<>());
    }

    /**
     * add acls to allACLs
     * @param principal kafka user name
     * @param aclBinding {@link AclBinding}
     */
    public static void addACLs(String principal, AclBinding aclBinding) {
        Vector<AclBinding> aclBindings = FakeLocalMetadataStore.allAcls.getOrDefault(principal, new Vector<>());
        aclBindings.add(aclBinding);
        FakeLocalMetadataStore.allAcls.putIfAbsent(principal, aclBindings);
    }

    /**
     * clear allTopics and allAcls.
     */
    public static void clear() {
        allTopics.clear();
        allAcls.clear();
    }
}
