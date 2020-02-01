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
package kafka.examples;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This exactly once demo driver is following below steps:
 * 1. Set up a producer to pre populate a set of records into input topic
 * 2.
 */
public class KafkaExactlyOnceDemo {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length != 4) {
            throw new IllegalArgumentException("Should accept 4 parameters: [mode], " +
                "[number of partitions], [number of instances], [number of records]");
        }

        String mode = args[0];
        int numPartitions = Integer.valueOf(args[1]);
        int numInstances = Integer.valueOf(args[2]);
        int numRecords = Integer.valueOf(args[3]);

        recreateTopics(numPartitions);

        CountDownLatch prePopulateLatch = new CountDownLatch(1);

        // Pre-populate records.
        final boolean isAsync = false;
        final boolean enableIdempotency = true;
        Producer producerThread = new Producer(INPUT_TOPIC, isAsync, null, enableIdempotency, numRecords, prePopulateLatch);
        producerThread.start();

        prePopulateLatch.await(5, TimeUnit.MINUTES);

        CountDownLatch transactionalCopyLatch = new CountDownLatch(numInstances);

        // Transactionally copy over all messages.
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            ExactlyOnceMessageProcessor messageProcessor = new ExactlyOnceMessageProcessor(mode,
                INPUT_TOPIC, OUTPUT_TOPIC, numPartitions,
                numInstances, instanceIdx, transactionalCopyLatch);
            messageProcessor.start();
        }

        transactionalCopyLatch.await(5, TimeUnit.MINUTES);

        CountDownLatch consumeLatch = new CountDownLatch(1);

        Consumer consumerThread = new Consumer(OUTPUT_TOPIC, "Verify-consumer", true, numRecords, consumeLatch);
        consumerThread.start();

        consumeLatch.await(5, TimeUnit.MINUTES);
        consumerThread.shutdown();
        System.out.println("All finished!");
    }

    private static void recreateTopics(final int numPartitions) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);

        Admin adminClient = Admin.create(props);

        List<String> topicsToDelete = Arrays.asList(INPUT_TOPIC, OUTPUT_TOPIC);

        try {
            adminClient.deleteTopics(topicsToDelete).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
            System.out.println("Encountered exception during topic deletion: " + e.getCause());
        }
        System.out.println("Deleted old topics: " + topicsToDelete);

        // Check topic existence in a retry loop
        while (true) {
            System.out.println("Making sure the topics are deleted successfully: " + topicsToDelete);
            boolean noTopicsInfo = true;

            Set<String> listedTopics = adminClient.listTopics().names().get();
            System.out.println("Current list of topics: " + listedTopics);

            for (String topic : topicsToDelete) {
               System.out.println("Checking topic " + topic);
               if (listedTopics.contains(topic)) {
                   noTopicsInfo = false;
                   break;
               }
            }

            if (noTopicsInfo) {
                break;
            }
            Thread.sleep(1000);
        }

        // Create topics in a retry loop
        while (true) {
            final short replicationFactor = 1;
            final List<NewTopic> newTopics = Arrays.asList(
                new NewTopic(INPUT_TOPIC, numPartitions, replicationFactor),
                new NewTopic(OUTPUT_TOPIC, numPartitions, replicationFactor));
            try {
                adminClient.createTopics(newTopics).all().get();
                System.out.println("Created new topics: " + newTopics);
                break;
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
                System.out.println("Metadata of the old topics are not cleared yet...");
                Thread.sleep(1000);
            }
        }
    }
}

//            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicsToDelete);
//            for (KafkaFuture<TopicDescription> future : describeTopicsResult.values().values()) {
//                try {
//                    TopicDescription description = future.get();
//                    System.out.println("Found topic description: " + description);
//                    noTopicsInfo = false;
//                    break;
//                } catch (ExecutionException e) {
//                    if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
//                        throw e;
//                    }
//                }
//            }

