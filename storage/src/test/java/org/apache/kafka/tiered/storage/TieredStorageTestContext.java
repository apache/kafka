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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.tiered.storage.specs.ExpandPartitionCountSpec;
import org.apache.kafka.tiered.storage.specs.TopicSpec;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;
import kafka.log.UnifiedLog;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageHistory;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageSnapshot;
import scala.Function0;
import scala.Function1;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import scala.Option;
import scala.collection.JavaConverters;

import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

public final class TieredStorageTestContext implements AutoCloseable {

    private final TieredStorageTestHarness harness;
    private final Serializer<String> ser = new StringSerializer();
    private final Deserializer<String> de = new StringDeserializer();
    private final Map<String, TopicSpec> topicSpecs = new HashMap<>();
    private final TieredStorageTestReport testReport;

    private volatile KafkaProducer<String, String> producer;
    private volatile Consumer<String, String> consumer;
    private volatile Admin admin;
    private volatile List<LocalTieredStorage> remoteStorageManagers;
    private volatile List<BrokerLocalStorage> localStorages;

    public TieredStorageTestContext(TieredStorageTestHarness harness) {
        this.harness = harness;
        this.testReport = new TieredStorageTestReport(this);
        initClients();
        initContext();
    }

    @SuppressWarnings("deprecation")
    private void initClients() {
        // rediscover the new bootstrap-server port incase of broker restarts
        ListenerName listenerName = harness.listenerName();
        Properties commonOverrideProps = new Properties();
        commonOverrideProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, harness.bootstrapServers(listenerName));

        // Set a producer linger of 60 seconds, in order to optimistically generate batches of
        // records with a pre-determined size.
        Properties producerOverrideProps = new Properties();
        producerOverrideProps.put(LINGER_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(60)));
        producerOverrideProps.putAll(commonOverrideProps);

        producer = harness.createProducer(ser, ser, producerOverrideProps);
        consumer = harness.createConsumer(de, de, commonOverrideProps,
                JavaConverters.asScalaBuffer(Collections.<String>emptyList()).toList());
        admin = harness.createAdminClient(listenerName, commonOverrideProps);
    }

    private void initContext() {
        remoteStorageManagers = TieredStorageTestHarness.remoteStorageManagers(harness.aliveBrokers());
        localStorages = TieredStorageTestHarness.localStorages(harness.aliveBrokers());
    }

    public void createTopic(TopicSpec spec) throws ExecutionException, InterruptedException {
        NewTopic newTopic;
        if (spec.getAssignment() == null || spec.getAssignment().isEmpty()) {
            newTopic = new NewTopic(spec.getTopicName(), spec.getPartitionCount(), (short) spec.getReplicationFactor());
        } else {
            Map<Integer, List<Integer>> replicasAssignments = spec.getAssignment();
            newTopic = new NewTopic(spec.getTopicName(), replicasAssignments);
        }
        newTopic.configs(spec.getProperties());
        admin.createTopics(Collections.singletonList(newTopic)).all().get();
        TestUtils.waitForAllPartitionsMetadata(harness.brokers(), spec.getTopicName(), spec.getPartitionCount());
        synchronized (this) {
            topicSpecs.put(spec.getTopicName(), spec);
        }
    }

    public void createPartitions(ExpandPartitionCountSpec spec) throws ExecutionException, InterruptedException {
        NewPartitions newPartitions;
        if (spec.getAssignment() == null || spec.getAssignment().isEmpty()) {
            newPartitions = NewPartitions.increaseTo(spec.getPartitionCount());
        } else {
            Map<Integer, List<Integer>> assignment = spec.getAssignment();
            List<List<Integer>> newAssignments = assignment.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            newPartitions = NewPartitions.increaseTo(spec.getPartitionCount(), newAssignments);
        }
        Map<String, NewPartitions> partitionsMap = Collections.singletonMap(spec.getTopicName(), newPartitions);
        admin.createPartitions(partitionsMap).all().get();
        TestUtils.waitForAllPartitionsMetadata(harness.brokers(), spec.getTopicName(), spec.getPartitionCount());
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
                Collections.singletonMap(configResource, alterEntries);
        admin.incrementalAlterConfigs(configsMap, alterOptions).all().get(30, TimeUnit.SECONDS);
    }

    public void deleteTopic(String topic) {
        TestUtils.deleteTopicWithAdmin(admin, topic, harness.brokers(), harness.controllerServers());
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
                                                        Long fetchOffset) {
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, fetchOffset);

        long timeoutMs = 60_000L;
        String sep = System.lineSeparator();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        Function1<ConsumerRecords<String, String>, Object> pollAction = polledRecords -> {
            polledRecords.forEach(records::add);
            return records.size() >= expectedTotalCount;
        };
        Function0<String> messageSupplier = () ->
                String.format("Could not consume %d records of %s from offset %d in %d ms. %d message(s) consumed:%s%s",
                        expectedTotalCount, topicPartition, fetchOffset, timeoutMs, records.size(), sep,
                        Utils.join(records, sep));
        TestUtils.pollRecordsUntilTrue(consumer, pollAction, messageSupplier, timeoutMs);
        return records;
    }

    public Long nextOffset(TopicPartition topicPartition) {
        List<TopicPartition> partitions = Collections.singletonList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        return consumer.position(topicPartition);
    }

    public Long beginOffset(TopicPartition topicPartition) {
        List<TopicPartition> partitions = Collections.singletonList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        return consumer.position(topicPartition);
    }

    public void bounce(int brokerId) {
        harness.killBroker(brokerId);
        boolean allBrokersDead = harness.aliveBrokers().isEmpty();
        harness.startBroker(brokerId);
        if (allBrokersDead) {
            reinitClients();
        }
        initContext();
    }

    public void stop(int brokerId) {
        harness.killBroker(brokerId);
        initContext();
    }

    public void start(int brokerId) {
        boolean allBrokersDead = harness.aliveBrokers().isEmpty();
        harness.startBroker(brokerId);
        if (allBrokersDead) {
            reinitClients();
        }
        initContext();
    }

    public void eraseBrokerStorage(int brokerId) throws IOException {
        localStorages.get(brokerId).eraseStorage();
    }

    public TopicSpec topicSpec(String topicName) {
        synchronized (topicSpecs) {
            return topicSpecs.get(topicName);
        }
    }

    public LocalTieredStorageSnapshot takeTieredStorageSnapshot() {
        int aliveBrokerId = harness.aliveBrokers().head().config().brokerId();
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

    public List<LocalTieredStorage> remoteStorageManagers() {
        return remoteStorageManagers;
    }

    public List<BrokerLocalStorage> localStorages() {
        return localStorages;
    }

    public Deserializer<String> de() {
        return de;
    }

    public Admin admin() {
        return admin;
    }

    public boolean isActive(Integer brokerId) {
        return harness.aliveBrokers().exists(b -> b.config().brokerId() == brokerId);
    }

    public boolean isAssignedReplica(TopicPartition topicPartition, Integer replicaId)
            throws ExecutionException, InterruptedException {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        TopicDescription description = admin.describeTopics(Collections.singletonList(topicPartition.topic()))
                .allTopicNames().get().get(topic);
        TopicPartitionInfo partitionInfo = description.partitions().get(partition);
        return partitionInfo.replicas().stream().anyMatch(node -> node.id() == replicaId);
    }

    public Optional<UnifiedLog> log(Integer brokerId, TopicPartition partition) {
        Option<UnifiedLog> log = harness.brokers().apply(brokerId).logManager().getLog(partition, false);
        return log.isDefined() ? Optional.of(log.get()) : Optional.empty();
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
        // IntegrationTestHarness closes the clients on tearDown, no need to close them explicitly.
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
