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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This exactly once demo driver takes 3 arguments:
 *   - partition: number of partitions for input/output topic
 *   - instances: number of instances
 *   - records: number of records
 * An example argument list would be `6 3 50000`.
 *
 * If you are using IntelliJ IDEA, the above arguments should be put in the configuration's `Program Arguments`.
 * Also recommended to set an output log file by `Edit Configuration -> Logs -> Save console
 * output to file` to record all the log output together.
 *
 * The driver could be decomposed as following stages:
 *
 * 1. Cleanup any topic whose name conflicts with input and output topic, so that we have a clean-start.
 *
 * 2. Set up a producer in a separate thread to pre-populate a set of records with even number keys into
 *    the input topic. The driver will block for the record generation to finish, so the producer
 *    must be in synchronous sending mode.
 *
 * 3. Set up transactional instances in separate threads which does a consume-process-produce loop,
 *    tailing data from input topic (See {@link ExactlyOnceMessageProcessor}). Each EOS instance will
 *    drain all the records from either given partitions or auto assigned partitions by actively
 *    comparing log end offset with committed offset. Each record will be processed exactly once
 *    as dividing the key by 2, and extend the value message. The driver will block for all the record
 *    processing to finish. The transformed record shall be written to the output topic, with
 *    transactional guarantee.
 *
 * 4. Set up a read committed consumer in a separate thread to verify we have all records within
 *    the output topic, while the message ordering on partition level is maintained.
 *    The driver will block for the consumption of all committed records.
 *
 * From this demo, you could see that all the records from pre-population are processed exactly once,
 * with strong partition level ordering guarantee.
 *
 * Note: please start the kafka broker and zookeeper in local first. The broker version must be >= 2.5
 * in order to run, otherwise the app could throw
 * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
 */
public class KafkaExactlyOnceDemo {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length != 3) {
            throw new IllegalArgumentException("Should accept 3 parameters: " +
                "[number of partitions], [number of instances], [number of records]");
        }

        int numPartitions = Integer.parseInt(args[0]);
        int numInstances = Integer.parseInt(args[1]);
        int numRecords = Integer.parseInt(args[2]);

        /* Stage 1: topic cleanup and recreation */
        recreateTopics(numPartitions);

        CountDownLatch prePopulateLatch = new CountDownLatch(1);

        /* Stage 2: pre-populate records */
        Producer producerThread = new Producer(INPUT_TOPIC, false, null, true, numRecords, -1, prePopulateLatch);
        producerThread.start();

        if (!prePopulateLatch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for data pre-population");
        }

        CountDownLatch transactionalCopyLatch = new CountDownLatch(numInstances);

        /* Stage 3: transactionally process all messages */
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            ExactlyOnceMessageProcessor messageProcessor = new ExactlyOnceMessageProcessor(
                INPUT_TOPIC, OUTPUT_TOPIC, instanceIdx, transactionalCopyLatch);
            messageProcessor.start();
        }

        if (!transactionalCopyLatch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for transactionally message copy");
        }

        CountDownLatch consumeLatch = new CountDownLatch(1);

        /* Stage 4: consume all processed messages to verify exactly once */
        Consumer consumerThread = new Consumer(OUTPUT_TOPIC, "Verify-consumer", Optional.empty(), true, numRecords, consumeLatch);
        consumerThread.start();

        if (!consumeLatch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for output data consumption");
        }

        consumerThread.shutdown();
        System.out.println("All finished!");
    }

    private static void recreateTopics(final int numPartitions)
        throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);

        Admin adminClient = Admin.create(props);

        List<String> topicsToDelete = Arrays.asList(INPUT_TOPIC, OUTPUT_TOPIC);

        deleteTopic(adminClient, topicsToDelete);

        // Check topic existence in a retry loop
        while (true) {
            System.out.println("Making sure the topics are deleted successfully: " + topicsToDelete);

            Set<String> listedTopics = adminClient.listTopics().names().get();
            System.out.println("Current list of topics: " + listedTopics);

            boolean hasTopicInfo = false;
            for (String listedTopic : listedTopics) {
                if (topicsToDelete.contains(listedTopic)) {
                    hasTopicInfo = true;
                    break;
                }
            }
            if (!hasTopicInfo) {
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

                deleteTopic(adminClient, topicsToDelete);

                Thread.sleep(1000);
            }
        }
    }

    private static void deleteTopic(final Admin adminClient, final List<String> topicsToDelete)
        throws InterruptedException, ExecutionException {
        try {
            adminClient.deleteTopics(topicsToDelete).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
            System.out.println("Encountered exception during topic deletion: " + e.getCause());
        }
        System.out.println("Deleted old topics: " + topicsToDelete);
    }
}
