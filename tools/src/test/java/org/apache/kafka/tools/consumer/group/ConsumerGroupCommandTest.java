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
package org.apache.kafka.tools.consumer.group;

import kafka.api.BaseConsumerTest;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.provider.Arguments;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ConsumerGroupCommandTest extends kafka.integration.KafkaServerTestHarness {
    public static final String TOPIC = "foo";
    public static final String GROUP = "test.group";
    public static final String PROTOCOL_GROUP = "protocol-group";

    List<ConsumerGroupCommand.ConsumerGroupService> consumerGroupService = new ArrayList<>();
    List<AbstractConsumerGroupExecutor> consumerGroupExecutors = new ArrayList<>();

    @Override
    public Seq<kafka.server.KafkaConfig> generateConfigs() {
        List<KafkaConfig> cfgs = new ArrayList<>();

        TestUtils.createBrokerConfigs(
            1,
            zkConnectOrNull(),
            false,
            true,
            scala.None$.empty(),
            scala.None$.empty(),
            scala.None$.empty(),
            true,
            false,
            false,
            false,
            scala.collection.immutable.Map$.MODULE$.empty(),
            1,
            false,
            1,
            (short) 1,
            0,
            false
        ).foreach(props -> {
            props.setProperty(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, isNewGroupCoordinatorEnabled() + "");
            cfgs.add(KafkaConfig.fromProps(props));
            return null;
        });

        return seq(cfgs);
    }

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        createTopic(TOPIC, 1, 1, new Properties(), listenerName(), new Properties());
    }

    @AfterEach
    @Override
    public void tearDown() {
        consumerGroupService.forEach(ConsumerGroupCommand.ConsumerGroupService::close);
        consumerGroupExecutors.forEach(AbstractConsumerGroupExecutor::shutdown);
        super.tearDown();
    }

    Map<TopicPartition, Long> committedOffsets(String topic, String group) {
        try (Consumer<String, String> consumer = createNoAutoCommitConsumer(group)) {
            Set<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toSet());
            return consumer.committed(partitions).entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        }
    }

    Consumer<String, String> createNoAutoCommitConsumer(String group) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers(listenerName()));
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        ConsumerGroupCommand.ConsumerGroupService service = new ConsumerGroupCommand.ConsumerGroupService(
            opts,
            Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );

        consumerGroupService.add(0, service);
        return service;
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers) {
        return addConsumerGroupExecutor(numConsumers, TOPIC, GROUP, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), false, GroupProtocol.CLASSIC.name);
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String groupProtocol) {
        return addConsumerGroupExecutor(numConsumers, TOPIC, GROUP, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), false, groupProtocol);
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String groupProtocol, Optional<String> remoteAssignor) {
        return addConsumerGroupExecutor(numConsumers, TOPIC, GROUP, RangeAssignor.class.getName(), remoteAssignor, Optional.empty(), false, groupProtocol);
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String group, String groupProtocol) {
        return addConsumerGroupExecutor(numConsumers, TOPIC, group, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), false, groupProtocol);
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String topic, String group, String groupProtocol) {
        return addConsumerGroupExecutor(numConsumers, topic, group, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), false, groupProtocol);
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String topic, String group, String strategy, Optional<String> remoteAssignor,
                                                   Optional<Properties> customPropsOpt, boolean syncCommit, String groupProtocol) {
        ConsumerGroupExecutor executor = new ConsumerGroupExecutor(bootstrapServers(listenerName()), numConsumers, group, groupProtocol,
            topic, strategy, remoteAssignor, customPropsOpt, syncCommit);
        addExecutor(executor);
        return executor;
    }

    SimpleConsumerGroupExecutor addSimpleGroupExecutor(String group) {
        return addSimpleGroupExecutor(Arrays.asList(new TopicPartition(TOPIC, 0)), group);
    }

    SimpleConsumerGroupExecutor addSimpleGroupExecutor(Collection<TopicPartition> partitions, String group) {
        SimpleConsumerGroupExecutor executor = new SimpleConsumerGroupExecutor(bootstrapServers(listenerName()), group, partitions);
        addExecutor(executor);
        return executor;
    }

    private AbstractConsumerGroupExecutor addExecutor(AbstractConsumerGroupExecutor executor) {
        consumerGroupExecutors.add(0, executor);
        return executor;
    }

    abstract class AbstractConsumerRunnable implements Runnable {
        final String broker;
        final String groupId;
        final Optional<Properties> customPropsOpt;
        final boolean syncCommit;

        final Properties props = new Properties();
        KafkaConsumer<String, String> consumer;

        boolean configured = false;

        public AbstractConsumerRunnable(String broker, String groupId, Optional<Properties> customPropsOpt, boolean syncCommit) {
            this.broker = broker;
            this.groupId = groupId;
            this.customPropsOpt = customPropsOpt;
            this.syncCommit = syncCommit;
        }

        void configure() {
            configured = true;
            configure(props);
            customPropsOpt.ifPresent(props::putAll);
            consumer = new KafkaConsumer<>(props);
        }

        void configure(Properties props) {
            props.put("bootstrap.servers", broker);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
        }

        abstract void subscribe();

        @Override
        public void run() {
            assert configured : "Must call configure before use";
            try {
                subscribe();
                while (true) {
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    if (syncCommit)
                        consumer.commitSync();
                }
            } catch (WakeupException e) {
                // OK
            } finally {
                consumer.close();
            }
        }

        void shutdown() {
            consumer.wakeup();
        }
    }

    class ConsumerRunnable extends AbstractConsumerRunnable {
        final String topic;
        final String groupProtocol;
        final String strategy;
        final Optional<String> remoteAssignor;

        public ConsumerRunnable(String broker, String groupId, String groupProtocol, String topic, String strategy,
                                Optional<String> remoteAssignor, Optional<Properties> customPropsOpt, boolean syncCommit) {
            super(broker, groupId, customPropsOpt, syncCommit);

            this.topic = topic;
            this.groupProtocol = groupProtocol;
            this.strategy = strategy;
            this.remoteAssignor = remoteAssignor;
        }

        @Override
        void configure(Properties props) {
            super.configure(props);
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
            if (groupProtocol.toUpperCase(Locale.ROOT).equals(GroupProtocol.CONSUMER.toString())) {
                remoteAssignor.ifPresent(assignor -> props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, assignor));
            } else {
                props.put("partition.assignment.strategy", strategy);
            }
        }

        @Override
        void subscribe() {
            consumer.subscribe(Collections.singleton(topic));
        }
    }

    class SimpleConsumerRunnable extends AbstractConsumerRunnable {
        final Collection<TopicPartition> partitions;

        public SimpleConsumerRunnable(String broker, String groupId, Collection<TopicPartition> partitions) {
            super(broker, groupId, Optional.empty(), false);

            this.partitions = partitions;
        }

        @Override
        void subscribe() {
            consumer.assign(partitions);
        }
    }

    class AbstractConsumerGroupExecutor {
        final int numThreads;
        final ExecutorService executor;
        final List<AbstractConsumerRunnable> consumers = new ArrayList<>();

        public AbstractConsumerGroupExecutor(int numThreads) {
            this.numThreads = numThreads;
            this.executor = Executors.newFixedThreadPool(numThreads);
        }

        void submit(AbstractConsumerRunnable consumerThread) {
            consumers.add(consumerThread);
            executor.submit(consumerThread);
        }

        void shutdown() {
            consumers.forEach(AbstractConsumerRunnable::shutdown);
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    class ConsumerGroupExecutor extends AbstractConsumerGroupExecutor {
        public ConsumerGroupExecutor(String broker, int numConsumers, String groupId, String groupProtocol, String topic, String strategy,
                                     Optional<String> remoteAssignor, Optional<Properties> customPropsOpt, boolean syncCommit) {
            super(numConsumers);
            IntStream.rangeClosed(1, numConsumers).forEach(i -> {
                ConsumerRunnable th = new ConsumerRunnable(broker, groupId, groupProtocol, topic, strategy, remoteAssignor, customPropsOpt, syncCommit);
                th.configure();
                submit(th);
            });
        }
    }

    class SimpleConsumerGroupExecutor extends AbstractConsumerGroupExecutor {
        public SimpleConsumerGroupExecutor(String broker, String groupId, Collection<TopicPartition> partitions) {
            super(1);

            SimpleConsumerRunnable th = new SimpleConsumerRunnable(broker, groupId, partitions);
            th.configure();
            submit(th);
        }
    }


    public static Stream<Arguments> getTestQuorumAndGroupProtocolParametersAll() {
        return BaseConsumerTest.getTestQuorumAndGroupProtocolParametersAll();
    }

    public static Stream<Arguments> getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly() {
        return BaseConsumerTest.getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly();
    }

    public static Stream<Arguments> getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly() {
        return BaseConsumerTest.getTestQuorumAndGroupProtocolParametersConsumerGroupProtocolOnly();
    }

    @SuppressWarnings({"deprecation"})
    static <T> Seq<T> seq(Collection<T> seq) {
        return JavaConverters.asScalaIteratorConverter(seq.iterator()).asScala().toSeq();
    }
}
