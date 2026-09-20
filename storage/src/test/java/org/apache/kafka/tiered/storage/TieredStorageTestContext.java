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
package org.apache.kafka.tiered.storage;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageHistory;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageSnapshot;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.specs.ExpandPartitionCountSpec;
import org.apache.kafka.tiered.storage.specs.TopicSpec;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.STORAGE_WAIT_TIMEOUT_SEC;

public final class TieredStorageTestContext implements AutoCloseable {

    private final ClusterInstance cluster;
    private final Map<String, Object> extraConsumerProps;
    private final Map<String, TopicSpec> topicSpecs = new HashMap<>();
    private final TieredStorageTestReport testReport;

    private volatile Producer<String, String> producer;
    private volatile Consumer<String, String> consumer;
    private volatile Admin admin;
    private volatile List<LocalTieredStorage> remoteStorageManagers;
    private volatile List<BrokerLocalStorage> localStorages;

    public TieredStorageTestContext(ClusterInstance cluster, Map<String, Object> extraConsumerProps) {
        this.cluster = cluster;
        this.extraConsumerProps = extraConsumerProps;
        this.testReport = new TieredStorageTestReport(this);
        initClients();
        initContext();
    }

    private void initClients() {
        // Set a producer linger of 60 seconds, in order to optimistically generate batches of
        // records with a pre-determined size.
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(LINGER_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(60)));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.putAll(extraConsumerProps);

        producer = cluster.producer(producerProps);
        consumer = cluster.consumer(consumerProps);
        admin = cluster.admin(Map.of());
    }

    private void initContext() {
        remoteStorageManagers = remoteStorageManagers(cluster.aliveBrokers().values());
        localStorages = localStorages(cluster.aliveBrokers().values());
    }

    private static List<LocalTieredStorage> remoteStorageManagers(Collection<KafkaBroker> brokers) {
        List<LocalTieredStorage> storages = new ArrayList<>();
        brokers.forEach(broker -> {
            if (broker.remoteLogManagerOpt().isDefined()) {
                RemoteLogManager remoteLogManager = broker.remoteLogManagerOpt().get();
                RemoteStorageManager storageManager = remoteLogManager.storageManager();
                if (storageManager instanceof ClassLoaderAwareRemoteStorageManager loaderAwareRSM) {
                    if (loaderAwareRSM.delegate() instanceof LocalTieredStorage) {
                        storages.add((LocalTieredStorage) loaderAwareRSM.delegate());
                    }
                } else if (storageManager instanceof LocalTieredStorage) {
                    storages.add((LocalTieredStorage) storageManager);
                }
            } else {
                throw new AssertionError("Broker " + broker.config().brokerId()
                        + " does not have a remote log manager.");
            }
        });
        return storages;
    }

    private static List<BrokerLocalStorage> localStorages(Collection<KafkaBroker> brokers) {
        return brokers.stream()
                .map(b -> new BrokerLocalStorage(b.config().brokerId(), Set.copyOf(b.config().logDirs()),
                        STORAGE_WAIT_TIMEOUT_SEC))
                .toList();
    }

    public void createTopic(TopicSpec spec) throws ExecutionException, InterruptedException {
        NewTopic newTopic;
        if (spec.assignment() == null || spec.assignment().isEmpty()) {
            newTopic = new NewTopic(spec.topicName(), spec.partitionCount(), (short) spec.replicationFactor());
        } else {
            Map<Integer, List<Integer>> replicasAssignments = spec.assignment();
            newTopic = new NewTopic(spec.topicName(), replicasAssignments);
        }
        newTopic.configs(spec.properties());
        admin.createTopics(List.of(newTopic)).all().get();
        cluster.waitTopicCreation(spec.topicName(), spec.partitionCount());
        synchronized (this) {
            topicSpecs.put(spec.topicName(), spec);
        }
    }

    public void createPartitions(ExpandPartitionCountSpec spec) throws ExecutionException, InterruptedException {
        NewPartitions newPartitions;
        if (spec.assignment() == null || spec.assignment().isEmpty()) {
            newPartitions = NewPartitions.increaseTo(spec.partitionCount());
        } else {
            Map<Integer, List<Integer>> assignment = spec.assignment();
            List<List<Integer>> newAssignments = assignment.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue)
                    .toList();
            newPartitions = NewPartitions.increaseTo(spec.partitionCount(), newAssignments);
        }
        Map<String, NewPartitions> partitionsMap = Map.of(spec.topicName(), newPartitions);
        admin.createPartitions(partitionsMap).all().get();
        cluster.waitTopicCreation(spec.topicName(), spec.partitionCount());
    }

    public void updateTopicConfig(String topic,
                                  Map<String, String> configsToBeAdded,
                                  List<String> configsToBeDeleted)
            throws ExecutionException, InterruptedException, TimeoutException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        updateResource(configResource, configsToBeAdded, configsToBeDeleted);
    }

    public void updateBrokerConfig(Integer brokerId,
                                   Map<String, String> configsToBeAdded,
                                   List<String> configsToBeDeleted)
            throws ExecutionException, InterruptedException, TimeoutException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString());
        updateResource(configResource, configsToBeAdded, configsToBeDeleted);
    }

    private void updateResource(ConfigResource configResource,
                                Map<String, String> configsToBeAdded,
                                List<String> configsToBeDeleted)
            throws ExecutionException, InterruptedException, TimeoutException {
        List<AlterConfigOp> alterEntries = new ArrayList<>();
        configsToBeDeleted.forEach(k ->
                alterEntries.add(new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE)));
        configsToBeAdded.forEach((k, v) ->
                alterEntries.add(new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)));
        AlterConfigsOptions alterOptions = new AlterConfigsOptions().timeoutMs(30000);
        Map<ConfigResource, Collection<AlterConfigOp>> configsMap =
                Map.of(configResource, alterEntries);
        admin.incrementalAlterConfigs(configsMap, alterOptions).all().get(30, TimeUnit.SECONDS);
    }

    public void deleteTopic(String topic) throws InterruptedException, ExecutionException {
        admin.deleteTopics(List.of(topic)).all().get();
    }

    /**
     * Send the given records trying to honor the batch size. This is attempted
     * with a large producer linger and the use of an explicit flush every time
     * the number of a "group" of records reaches the batch size.
     * @param recordsToProduce the records to produce
     * @param batchSize the batch size
     */
    public void produce(List<ProducerRecord<String, String>> recordsToProduce, Integer batchSize) {
        int counter = 1;
        for (ProducerRecord<String, String> record : recordsToProduce) {
            producer.send(record);
            if (counter++ % batchSize == 0) {
                producer.flush();
            }
        }
        producer.flush();
    }

    public List<ConsumerRecord<String, String>> consume(TopicPartition topicPartition,
                                                        Integer expectedTotalCount,
                                                        Long fetchOffset) throws InterruptedException {
        consumer.assign(List.of(topicPartition));
        consumer.seek(topicPartition, fetchOffset);

        long timeoutMs = 60_000L;
        long pollTimeoutMs = 100L;
        String sep = System.lineSeparator();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        TestUtils.waitForCondition(
            () -> {
                consumer.poll(Duration.ofMillis(pollTimeoutMs)).forEach(records::add);
                return records.size() >= expectedTotalCount;
            },
            timeoutMs,
            () -> String.format("Could not consume %d records of %s from offset %d in %d ms. %d message(s) consumed:%s%s",
                    expectedTotalCount, topicPartition, fetchOffset, timeoutMs, records.size(), sep,
                    records.stream().map(Object::toString).collect(Collectors.joining(sep)))
        );
        return records;
    }

    public Long nextOffset(TopicPartition topicPartition) {
        List<TopicPartition> partitions = List.of(topicPartition);
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        return consumer.position(topicPartition);
    }

    public Long beginOffset(TopicPartition topicPartition) {
        List<TopicPartition> partitions = List.of(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        return consumer.position(topicPartition);
    }

    public void bounce(int brokerId) {
        cluster.shutdownBroker(brokerId);
        boolean allBrokersDead = cluster.aliveBrokers().isEmpty();
        cluster.startBroker(brokerId);
        if (allBrokersDead) {
            reinitClients();
        }
        initContext();
    }

    public void stop(int brokerId) {
        cluster.shutdownBroker(brokerId);
        initContext();
    }

    public void start(int brokerId) {
        boolean allBrokersDead = cluster.aliveBrokers().isEmpty();
        cluster.startBroker(brokerId);
        if (allBrokersDead) {
            reinitClients();
        }
        initContext();
    }

    public void eraseBrokerStorage(int brokerId,
                                   FilenameFilter filter,
                                   boolean isStopped) throws IOException {
        BrokerLocalStorage brokerLocalStorage;
        if (isStopped) {
            brokerLocalStorage = localStorages(cluster.brokers().values())
                    .stream()
                    .filter(bls -> bls.getBrokerId() == brokerId)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No local storage found for broker " + brokerId));
        } else {
            brokerLocalStorage = localStorages.get(brokerId);
        }
        brokerLocalStorage.eraseStorage(filter);
    }

    public TopicSpec topicSpec(String topicName) {
        synchronized (topicSpecs) {
            return topicSpecs.get(topicName);
        }
    }

    public LocalTieredStorageSnapshot takeTieredStorageSnapshot() {
        int aliveBrokerId = cluster.aliveBrokers().values().iterator().next().config().brokerId();
        return LocalTieredStorageSnapshot.takeSnapshot(remoteStorageManager(aliveBrokerId));
    }

    public LocalTieredStorageHistory tieredStorageHistory(int brokerId) {
        return remoteStorageManager(brokerId).getHistory();
    }

    public LocalTieredStorage remoteStorageManager(int brokerId) {
        return remoteStorageManagers.stream()
                .filter(rsm -> rsm.brokerId() == brokerId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No remote storage manager found for broker " + brokerId));
    }

    // unused now, but it can be reused later as this is an utility method.
    public Optional<LeaderEpochFileCache> leaderEpochFileCache(int brokerId, TopicPartition partition) {
        return log(brokerId, partition).map(UnifiedLog::leaderEpochCache);
    }

    public List<LocalTieredStorage> remoteStorageManagers() {
        return remoteStorageManagers;
    }

    public List<BrokerLocalStorage> localStorages() {
        return localStorages;
    }

    public Deserializer<String> deserializer() {
        return new StringDeserializer();
    }

    public Admin admin() {
        return admin;
    }

    public boolean isActive(Integer brokerId) {
        return cluster.aliveBrokers().containsKey(brokerId);
    }

    public boolean isAssignedReplica(TopicPartition topicPartition, Integer replicaId)
            throws ExecutionException, InterruptedException {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        TopicDescription description = admin.describeTopics(List.of(topicPartition.topic()))
                .allTopicNames().get().get(topic);
        TopicPartitionInfo partitionInfo = description.partitions().get(partition);
        return partitionInfo.replicas().stream().anyMatch(node -> node.id() == replicaId);
    }

    public Optional<UnifiedLog> log(Integer brokerId, TopicPartition partition) {
        return cluster.brokers().get(brokerId).logManager().getLog(partition, false);
    }

    public void succeed(TieredStorageTestAction action) {
        testReport.addSucceeded(action);
    }

    public void fail(TieredStorageTestAction action) {
        testReport.addFailed(action);
    }

    public void printReport(PrintStream output) {
        testReport.print(output);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(producer, "Producer client");
        Utils.closeQuietly(consumer, "Consumer client");
        Utils.closeQuietly(admin, "Admin client");
    }

    private void reinitClients() {
        // Broker uses a random port (TestUtils.RandomPort) for the listener. If the initial bootstrap-server config
        // becomes invalid, then the clients won't be able to reconnect to the cluster.
        // To avoid this, we reinitialize the clients after all the brokers are bounced.
        Utils.closeQuietly(producer, "Producer client");
        Utils.closeQuietly(consumer, "Consumer client");
        Utils.closeQuietly(admin, "Admin client");
        initClients();
    }
}
