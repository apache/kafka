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

package org.apache.kafka.common.test.api;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public interface Cluster {
    Type type();

    /**
     * The immutable cluster configuration used to create this cluster.
     */
    ClusterConfig config();

    /**
     * Return the set of all controller IDs configured for this test. For kraft, this
     * will return only the nodes which have the "controller" role enabled in `process.roles`.
     */
    Set<Integer> controllerIds();

    /**
     * Return the set of all broker IDs configured for this test.
     */
    Set<Integer> brokerIds();


    /**
     * The listener for this cluster as configured by {@link ClusterTest} or by {@link ClusterConfig}. If
     * unspecified by those sources, this will return the listener for the default security protocol PLAINTEXT
     */
    ListenerName clientListener();

    /**
     * The listener for the kraft cluster controller configured by controller.listener.names.
     */
    default Optional<ListenerName> controllerListenerName() {
        return Optional.empty();
    }

    /**
     * The broker connect string which can be used by clients for bootstrapping
     */
    String bootstrapServers();

    /**
     * The broker connect string which can be used by clients for bootstrapping to the controller quorum.
     */
    String bootstrapControllers();

    String clusterId();

    //---------------------------[producer/consumer/admin]---------------------------//

    <K, V> Producer<K, V> producer(Map<String, Object> configs);

    default <K, V> Producer<K, V> producer() {
        return producer(Map.of());
    }

    <K, V> Consumer<K, V> consumer(Map<String, Object> config);

    default <K, V> Consumer<K, V> consumer() {
        return consumer(Map.of());
    }

    <K, V> ShareConsumer<K, V> shareConsumer(Map<String, Object> configs);

    default <K, V> ShareConsumer<K, V> shareConsumer() {
        return shareConsumer(Map.of());
    }

    Admin admin(Map<String, Object> configs, boolean usingBootstrapControllers);

    Map<String, Object> setClientSaslConfig(Map<String, Object> configs);

    default Admin admin(Map<String, Object> configs) {
        return admin(configs, false);
    }

    default Admin admin() {
        return admin(Map.of(), false);
    }

    Set<GroupProtocol> supportedGroupProtocols();

    //---------------------------[modify]---------------------------//

    void start();

    void stop();

    void shutdownBroker(int brokerId);

    void startBroker(int brokerId);

    //---------------------------[wait]---------------------------//

    default void waitTopicDeletion(String topic) throws InterruptedException {
        waitForTopic(topic, 0);
    }

    default void createTopic(String topicName, int partitions, short replicas) throws InterruptedException {
        try (Admin admin = admin()) {
            admin.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, replicas)));
            waitForTopic(topicName, partitions);
        }
    }

    void waitForReadyBrokers() throws InterruptedException;

    void waitForTopic(String topic, int partitions) throws InterruptedException;

    /**
     * Returns the broker id of leader partition.
     */
    int getLeaderBrokerId(TopicPartition topicPartition) throws ExecutionException, InterruptedException;
}
