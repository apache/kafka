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
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

        createTopics(numPartitions);

        CountDownLatch prePopulateLatch = new CountDownLatch(1);

        // Pre-populate records
        Producer producerThread = new Producer(INPUT_TOPIC, true, null, numRecords, prePopulateLatch);
        producerThread.start();

        prePopulateLatch.await(5, TimeUnit.MINUTES);

        CountDownLatch transactionalCopyLatch = new CountDownLatch(numInstances);

        // Transactionally copy over all messages
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            ExactlyOnceMessageProcessor messageProcessor = new ExactlyOnceMessageProcessor(mode,
                INPUT_TOPIC, OUTPUT_TOPIC, numPartitions,
                numInstances, instanceIdx, transactionalCopyLatch);
            messageProcessor.start();
        }

        transactionalCopyLatch.await(5, TimeUnit.MINUTES);

        CountDownLatch consumeLatch = new CountDownLatch(1);

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, true, numRecords, consumeLatch);
        consumerThread.start();

        consumeLatch.await(5, TimeUnit.MINUTES);
        consumerThread.shutdown();
        System.out.println("All finished!");
    }

    private static void createTopics(final int numPartitions) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Admin adminClient = Admin.create(properties);
        short replicationFactor = 1;
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(
            new NewTopic(INPUT_TOPIC, numPartitions, replicationFactor),
            new NewTopic(OUTPUT_TOPIC, numPartitions, replicationFactor)));

        result.all().get();
    }
}
