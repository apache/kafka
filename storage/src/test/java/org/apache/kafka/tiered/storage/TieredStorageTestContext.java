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

import org.apache.kafka.tiered.storage.specs.ExpandPartitionCountSpec;
import org.apache.kafka.tiered.storage.specs.TopicSpec;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;
import kafka.log.UnifiedLog;
import kafka.server.KafkaBroker;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageHistory;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageSnapshot;
import scala.Function0;
import scala.Function1;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
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
import scala.collection.Seq;

import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

public final class TieredStorageTestContext {

    private final Seq<KafkaBroker> brokers;
    private final Properties producerConfig;
    private final Properties consumerConfig;
    private final Properties adminConfig;

    private final Serializer<String> ser;
    private final Deserializer<String> de;

    private final Map<String, TopicSpec> topicSpecs;
    private final TieredStorageTestReport testReport;

    private volatile KafkaProducer<String, String> producer;
    private volatile KafkaConsumer<String, String> consumer;
    private volatile Admin admin;
    private volatile List<LocalTieredStorage> tieredStorages;
    private volatile List<BrokerLocalStorage> localStorages;

    public TieredStorageTestContext(Seq<KafkaBroker> brokers,
                                    Properties producerConfig,
                                    Properties consumerConfig,
                                    Properties adminConfig) {
        this.brokers = brokers;
        this.producerConfig = producerConfig;
        this.consumerConfig = consumerConfig;
        this.adminConfig = adminConfig;
        this.ser = Serdes.String().serializer();
        this.de = Serdes.String().deserializer();
        this.topicSpecs = new HashMap<>();
        this.testReport = new TieredStorageTestReport(this);
        initContext();
    }

    private void initContext() {
        // Set a producer linger of 60 seconds, in order to optimistically generate batches of
        // records with a pre-determined size.
        producerConfig.put(LINGER_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(60)));
        producer = new KafkaProducer<>(producerConfig, ser, ser);
        consumer = new KafkaConsumer<>(consumerConfig, de, de);
        admin = Admin.create(adminConfig);

        tieredStorages = TieredStorageTestHarness.getTieredStorages(brokers);
        localStorages = TieredStorageTestHarness.getLocalStorages(brokers);
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

    public void deleteTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
        admin.deleteTopics(Collections.singletonList(topic)).all().get(60, TimeUnit.SECONDS);
    }

    /**
     * Send the given records trying to honor the batch size. This is attempted
     * with a large producer linger and the use of an explicit flush every time
     * the number of a "group" of records reaches the batch size.
     * @param recordsToProduce the records to produce
     * @param batchSize the batch size
     */
    public void produce(List<ProducerRecord<String, String>> recordsToProduce, Integer batchSize) {
        int counter = 0;
        for (ProducerRecord<String, String> record : recordsToProduce) {
            producer.send(record);
            if (counter++ % batchSize == 0) {
                producer.flush();
            }
        }
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
        KafkaBroker broker = brokers.apply(brokerId);
        closeClients();
        broker.shutdown();
        broker.awaitShutdown();
        broker.startup();
        initContext();
    }

    public void stop(int brokerId) {
        KafkaBroker broker = brokers.apply(brokerId);
        closeClients();
        broker.shutdown();
        broker.awaitShutdown();
        initContext();
    }

    public void start(int brokerId) {
        KafkaBroker broker = brokers.apply(brokerId);
        closeClients();
        broker.startup();
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
        return LocalTieredStorageSnapshot.takeSnapshot(tieredStorages.get(0));
    }

    public LocalTieredStorageHistory getTieredStorageHistory(int brokerId) {
        return tieredStorages.get(brokerId).getHistory();
    }

    public List<LocalTieredStorage> getTieredStorages() {
        return tieredStorages;
    }

    public List<BrokerLocalStorage> getLocalStorages() {
        return localStorages;
    }

    public Serializer<String> getSer() {
        return ser;
    }

    public Deserializer<String> getDe() {
        return de;
    }

    public Admin admin() {
        return admin;
    }

    public boolean isActive(Integer brokerId) {
        return brokers.apply(brokerId).brokerState().equals(BrokerState.RUNNING);
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
        Option<UnifiedLog> log = brokers.apply(brokerId).logManager().getLog(partition, false);
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

    public void close() throws IOException {
        Utils.closeAll(producer, consumer);
        admin.close();
    }

    private void closeClients() {
        producer.close(Duration.ofSeconds(5));
        consumer.close(Duration.ofSeconds(5));
        admin.close();
    }
}
