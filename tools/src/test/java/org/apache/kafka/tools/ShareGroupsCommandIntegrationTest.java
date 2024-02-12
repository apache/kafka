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
package org.apache.kafka.tools;

import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.tools.ShareGroupsCommand.ShareGroupService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ShareGroupsCommandIntegrationTest extends kafka.integration.KafkaServerTestHarness {
    public static final String TOPIC = "foo";

    List<ShareGroupService> shareGroupService = new ArrayList<>();
    List<ShareGroupExecutor> shareGroupExecutors = new ArrayList<>();

    @Override
    public Seq<KafkaConfig> generateConfigs() {
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
            if (isNewGroupCoordinatorEnabled()) {
                props.setProperty(KafkaConfig.NewGroupCoordinatorEnableProp(), "true");
            }

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
        shareGroupService.forEach(ShareGroupService::close);
        shareGroupExecutors.forEach(ShareGroupExecutor::shutdown);
        super.tearDown();
    }

    ShareGroupService getShareGroupService(String[] args, Admin adminClient) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        ShareGroupService service = new ShareGroupService(
                opts,
                Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)),
                adminClient
        );

        shareGroupService.add(0, service);
        return service;
    }

    ShareGroupExecutor addShareGroupExecutor(int numConsumers, String group) {
        ShareGroupExecutor executor = new ShareGroupExecutor(bootstrapServers(listenerName()), numConsumers, group,
                TOPIC, Optional.empty(), false);
        addExecutor(executor);
        return executor;
    }

    private ShareGroupExecutor addExecutor(ShareGroupExecutor executor) {
        shareGroupExecutors.add(0, executor);
        return executor;
    }

    class ShareConsumerRunnable implements Runnable {
        final String broker;
        final String groupId;
        final Optional<Properties> customPropsOpt;
        final boolean syncCommit;
        final String topic;

        final Properties props = new Properties();
        KafkaShareConsumer<String, String> consumer;

        boolean configured = false;

        public ShareConsumerRunnable(String broker, String groupId, Optional<Properties> customPropsOpt, boolean syncCommit, String topic) {
            this.broker = broker;
            this.groupId = groupId;
            this.customPropsOpt = customPropsOpt;
            this.syncCommit = syncCommit;
            this.topic = topic;
        }

        void configure() {
            configured = true;
            configure(props);
            customPropsOpt.ifPresent(props::putAll);
            consumer = new KafkaShareConsumer<>(props);
        }

        void configure(Properties props) {
            props.put("bootstrap.servers", broker);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
        }

        public void subscribe() {
            consumer.subscribe(Collections.singleton(topic));
        }

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

    class ShareGroupExecutor {
        final int numThreads;
        final ExecutorService executor;
        final List<ShareConsumerRunnable> consumers = new ArrayList<>();

        public ShareGroupExecutor(String broker, int numConsumers, String groupId, String topic, Optional<Properties> customPropsOpt, boolean syncCommit) {
            this.numThreads = numConsumers;
            this.executor = Executors.newFixedThreadPool(numThreads);
            IntStream.rangeClosed(1, numConsumers).forEach(i -> {
                ShareConsumerRunnable th = new ShareConsumerRunnable(broker, groupId, customPropsOpt, syncCommit, topic);
                th.configure();
                submit(th);
            });
        }

        void submit(ShareConsumerRunnable consumerThread) {
            consumers.add(consumerThread);
            executor.submit(consumerThread);
        }

        void shutdown() {
            consumers.forEach(ShareConsumerRunnable::shutdown);
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings({"deprecation"})
    static <T> Seq<T> seq(Collection<T> seq) {
        return JavaConverters.asScalaIteratorConverter(seq.iterator()).asScala().toSeq();
    }
}
