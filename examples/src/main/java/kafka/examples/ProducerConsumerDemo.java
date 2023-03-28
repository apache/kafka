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

import org.apache.kafka.common.errors.TimeoutException;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This producer and consumer demo takes just one optional argument called sync.
 * When sync is present, the producer will send messages synchronously, otherwise they will be sent asynchronously.
 *
 * If you are using IntelliJ IDEA, the above arguments should be put in `Modify Run Configuration - Program Arguments`.
 * You can also set an output log file in `Modify Run Configuration - Modify options - Save console output to file` to
 * record all the log output together.
 *
 * The driver can be decomposed into the following stages:
 *
 * 1. Set up a producer in a separate thread to send a set of messages with even number keys to the test topic.
 * 2. Set up a consumer in a separate thread to fetch all previously sent messages from the test topic.
 */
public class ProducerConsumerDemo {
    public static final String TOPIC = "topic1";
    public static final int NUM_MESSAGES = 10_000;

    public static void main(String[] args) throws InterruptedException {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        CountDownLatch latch = new CountDownLatch(2);

        Producer producerThread = new Producer(TOPIC, isAsync, null, false, NUM_MESSAGES, -1, latch);
        producerThread.start();

        Consumer consumerThread = new Consumer(TOPIC, "DemoConsumer", Optional.empty(), false, NUM_MESSAGES, latch);
        consumerThread.start();

        if (!latch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }

        consumerThread.shutdown();
        System.out.println("All finished!");
    }
}
