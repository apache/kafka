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

import kafka.log.UnifiedLog;
import kafka.network.SocketServer;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;

public interface ClusterInstance {

    Type type();

    Map<Integer, KafkaBroker> brokers();

    default Map<Integer, KafkaBroker> aliveBrokers() {
        return brokers().entrySet().stream().filter(entry -> !entry.getValue().isShutdown())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<Integer, ControllerServer> controllers();

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
    default Set<Integer> brokerIds() {
        return brokers().keySet();
    }

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

    /**
     * A collection of all brokers in the cluster.
     */
    default Collection<SocketServer> brokerSocketServers() {
        return brokers().values().stream()
                .map(KafkaBroker::socketServer)
                .collect(Collectors.toList());
    }

    /**
     * A collection of all controllers in the cluster.
     */
    Collection<SocketServer> controllerSocketServers();

    /**
     * Return any one of the broker servers. Throw an error if none are found
     */
    default SocketServer anyBrokerSocketServer() {
        return brokerSocketServers().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
    }

    /**
     * Return any one of the controller servers. Throw an error if none are found
     */
    default SocketServer anyControllerSocketServer() {
        return controllerSocketServers().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No controller SocketServers found"));
    }

    String clusterId();

    //---------------------------[producer/consumer/admin]---------------------------//

    default <K, V> Producer<K, V> producer(Map<String, Object> configs) {
        Map<String, Object> props = new HashMap<>(configs);
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaProducer<>(props);
    }

    default <K, V> Producer<K, V> producer() {
        return producer(Map.of());
    }

    default <K, V> Consumer<K, V> consumer(Map<String, Object> configs) {
        Map<String, Object> props = new HashMap<>(configs);
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "group_" + TestUtils.randomString(5));
        props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaConsumer<>(props);
    }

    default <K, V> Consumer<K, V> consumer() {
        return consumer(Map.of());
    }

    default Admin admin(Map<String, Object> configs, boolean usingBootstrapControllers) {
        Map<String, Object> props = new HashMap<>(configs);
        if (usingBootstrapControllers) {
            props.putIfAbsent(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, bootstrapControllers());
            props.remove(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        } else {
            props.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
            props.remove(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
        }
        return Admin.create(props);
    }

    default Admin admin(Map<String, Object> configs) {
        return admin(configs, false);
    }

    default Admin admin() {
        return admin(Map.of(), false);
    }

    default Set<GroupProtocol> supportedGroupProtocols() {
        if (brokers().values().stream().allMatch(b -> b.dataPlaneRequestProcessor().isConsumerGroupProtocolEnabled())) {
            return Set.of(CLASSIC, CONSUMER);
        } else {
            return Collections.singleton(CLASSIC);
        }
    }

    /**
     * Returns the first recorded fatal exception, if any.
     *
     */
    Optional<FaultHandlerException> firstFatalException();

    /**
     * Return the first recorded non-fatal exception, if any.
     */
    Optional<FaultHandlerException> firstNonFatalException();

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

    default void waitForTopic(String topic, int partitions) throws InterruptedException {
        // wait for metadata
        Collection<KafkaBroker> brokers = aliveBrokers().values();
        TestUtils.waitForCondition(
            () -> brokers.stream().allMatch(broker -> partitions == 0 ?
                broker.metadataCache().numPartitions(topic).isEmpty() :
                broker.metadataCache().numPartitions(topic).contains(partitions)
        ), 60000L, topic + " metadata not propagated after 60000 ms");

        for (ControllerServer controller : controllers().values()) {
            long controllerOffset = controller.raftManager().replicatedLog().endOffset().offset() - 1;
            TestUtils.waitForCondition(
                () -> brokers.stream().allMatch(broker -> ((BrokerServer) broker).sharedServer().loader().lastAppliedOffset() >= controllerOffset),
                60000L, "Timeout waiting for controller metadata propagating to brokers");
        }

        if (partitions == 0) {
            List<TopicPartition> topicPartitions = IntStream.range(0, 1)
                .mapToObj(partition -> new TopicPartition(topic, partition))
                .collect(Collectors.toList());

            // Ensure that the topic-partition has been deleted from all brokers' replica managers
            TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                    topicPartitions.stream().allMatch(tp -> broker.replicaManager().onlinePartition(tp).isEmpty())),
                "Replica manager's should have deleted all of this topic's partitions");

            // Ensure that logs from all replicas are deleted
            TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                    topicPartitions.stream().allMatch(tp -> broker.logManager().getLog(tp, false).isEmpty())),
                "Replica logs not deleted after delete topic is complete");

            // Ensure that the topic is removed from all cleaner offsets
            TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                    topicPartitions.stream().allMatch(tp -> {
                        List<File> liveLogDirs = CollectionConverters.asJava(broker.logManager().liveLogDirs());
                        return liveLogDirs.stream().allMatch(logDir -> {
                            OffsetCheckpointFile checkpointFile;
                            try {
                                checkpointFile = new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return !checkpointFile.read().containsKey(tp);
                        });
                    })),
                "Cleaner offset for deleted partition should have been removed");

            // Ensure that the topic directories are soft-deleted
            TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                    CollectionConverters.asJava(broker.config().logDirs()).stream().allMatch(logDir ->
                        topicPartitions.stream().noneMatch(tp ->
                            new File(logDir, tp.topic() + "-" + tp.partition()).exists()))),
                "Failed to soft-delete the data to a delete directory");

            // Ensure that the topic directories are hard-deleted
            TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                CollectionConverters.asJava(broker.config().logDirs()).stream().allMatch(logDir ->
                    topicPartitions.stream().allMatch(tp ->
                        Arrays.stream(Objects.requireNonNull(new File(logDir).list())).noneMatch(partitionDirectoryName ->
                            partitionDirectoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                                partitionDirectoryName.endsWith(UnifiedLog.DeleteDirSuffix())))
                )
            ), "Failed to hard-delete the delete directory");
        }
    }

    default List<Authorizer> authorizers() {
        List<Authorizer> authorizers = new ArrayList<>();
        authorizers.addAll(brokers().values().stream()
                .filter(server -> server.authorizer().isDefined())
                .map(server -> server.authorizer().get()).collect(Collectors.toList()));
        authorizers.addAll(controllers().values().stream()
                .filter(server -> server.authorizer().isDefined())
                .map(server -> server.authorizer().get()).collect(Collectors.toList()));
        return authorizers;
    }

    default void waitAcls(AclBindingFilter filter, Collection<AccessControlEntry> entries) throws InterruptedException {
        for (Authorizer authorizer : authorizers()) {
            AtomicReference<Set<AccessControlEntry>> actualEntries = new AtomicReference<>(new HashSet<>());
            TestUtils.waitForCondition(() -> {
                Set<AccessControlEntry> accessControlEntrySet = new HashSet<>();
                authorizer.acls(filter).forEach(aclBinding -> accessControlEntrySet.add(aclBinding.entry()));
                actualEntries.set(accessControlEntrySet);
                return accessControlEntrySet.containsAll(entries) && entries.containsAll(accessControlEntrySet);
            }, "expected acls: " + entries + ", actual acls: " + actualEntries.get());
        }
    }
}
