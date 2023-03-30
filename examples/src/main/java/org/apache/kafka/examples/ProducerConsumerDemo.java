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
package org.apache.kafka.examples;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This example can be decomposed into the following stages:
 *
 * 1. Clean any topics left from previous runs.
 * 2. Create a producer thread to send a set of messages to topic1.
 * 3. Create a consumer thread to fetch all previously sent messages from topic1.
 *
 * If you are using IntelliJ IDEA, the above arguments should be put in `Modify Run Configuration - Program Arguments`.
 * You can also set an output log file in `Modify Run Configuration - Modify options - Save console output to file` to
 * record all the log output together.
 */
public class ProducerConsumerDemo {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC_NAME = "topic1";

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                System.out.println("This example takes 2 arguments:\n" +
                    "- messages: total number of messages to send (required)\n" +
                    "- mode: pass \"sync\" to send messages synchronously (optional)\n" +
                    "An example argument list would be: 10000 sync");
                return;
            }

            int numRecords = Integer.parseInt(args[0]);
            boolean isAsync = args.length == 1 || !args[1].trim().equalsIgnoreCase("sync");

            // stage 1: recreate topics
            Utils.recreateTopics("localhost:9092", Arrays.asList(TOPIC_NAME), -1);
            CountDownLatch latch = new CountDownLatch(2);

            // stage 2: produce messages to topic1
            Producer producerThread = new Producer(BOOTSTRAP_SERVERS, TOPIC_NAME, isAsync, null, false, numRecords, -1, latch);
            producerThread.start();

            // stage 3: consume messages from topic1
            Consumer consumerThread = new Consumer(BOOTSTRAP_SERVERS, TOPIC_NAME, "my-group", Optional.empty(), false, numRecords, latch);
            consumerThread.start();

            if (!latch.await(5, TimeUnit.MINUTES)) {
                System.err.println("Timeout after 5 minutes waiting for termination");
                consumerThread.shutdown();
                producerThread.shutdown();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
