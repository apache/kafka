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

import org.apache.kafka.common.errors.TimeoutException;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This example can be decomposed into the following stages:
 *
 * 1. Clean any topics left from previous runs.
 * 2. Set up a producer thread to pre-populate a set of messages with even number keys into the input topic.
 *    The demo will block for the message generation to finish, so the producer is synchronous.
 * 3. Set up the transactional instances in separate threads, each one executing a read-process-write loop
 *    (See {@link MessageProcessor}). Each EOS instance will drain all messages from either given
 *    partitions or auto assigned partitions by actively comparing log end offset with committed offset.
 *    Each message will be processed exactly once, dividing the key by 2 and extending the value message.
 *    The demo will block until all messages are processed and written to the output topic.
 * 4. Create a read_committed consumer  thread to verify we have all records in the output topic,
 *    and message ordering at the partition level is maintained.
 *    The demo will block for the consumption of all committed messages.
 *
 * Broker version must be >= 2.5.0 in order to run, otherwise the example will throw
 * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
 *
 * If you are using IntelliJ IDEA, the above arguments should be put in `Modify Run Configuration - Program Arguments`.
 * You can also set an output log file in `Modify Run Configuration - Modify options - Save console output to file` to
 * record all the log output together.
 */
public class ExactlyOnceDemo {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                System.out.println("This example takes 3 parameters (i.e. 6 3 10000):\n" +
                    "- partition: number of partitions for input and output topics (required)\n" +
                    "- instances: number of application instances (required)\n" +
                    "- messages: total number of messages (required)");
                return;
            }

            int numPartitions = Integer.parseInt(args[0]);
            int numInstances = Integer.parseInt(args[1]);
            int numRecords = Integer.parseInt(args[2]);

            // stage 1: recreate topics
            Utils.recreateTopics("localhost:9092", Arrays.asList(INPUT_TOPIC, OUTPUT_TOPIC), numPartitions);
            CountDownLatch prePopulateLatch = new CountDownLatch(1);

            // stage 2: send demo messages to the input-topic
            Producer producerThread = new Producer(
                BOOTSTRAP_SERVERS, INPUT_TOPIC, false, null, true, numRecords, -1, prePopulateLatch);
            producerThread.start();

            if (!prePopulateLatch.await(5, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout after 5 minutes waiting for data load");
            }

            CountDownLatch transactionalCopyLatch = new CountDownLatch(numInstances);

            // stage 3: read from input-topic, process exactly-once and write to the output-topic
            for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
                MessageProcessor processor = new MessageProcessor(
                    BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC, instanceIdx, transactionalCopyLatch);
                processor.start();
            }

            if (!transactionalCopyLatch.await(5, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout after 5 minutes waiting for message copy");
            }

            CountDownLatch consumeLatch = new CountDownLatch(1);

            // stage 4: check by consuming all messages from the output-topic
            Consumer consumerThread = new Consumer(
                BOOTSTRAP_SERVERS, OUTPUT_TOPIC, "check-group", Optional.empty(), true, numRecords, consumeLatch);
            consumerThread.start();

            if (!consumeLatch.await(5, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout after 5 minutes waiting for output read");
            }

            consumerThread.shutdown();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
