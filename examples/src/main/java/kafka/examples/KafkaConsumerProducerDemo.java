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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This example can be decomposed into the following stages:
 * <p>
 * 1. Clean any topics left from previous runs.
 * 2. Create a producer thread to send a set of records to topic1.
 * 3. Create a consumer thread to fetch all previously sent records from topic1.
 * <p>
 * If you are using IntelliJ IDEA, the above arguments should be put in `Modify Run Configuration - Program Arguments`.
 * You can also set an output log file in `Modify Run Configuration - Modify options - Save console output to file` to
 * record all the log output together.
 */
public class KafkaConsumerProducerDemo {
    public static final String TOPIC_NAME = "my-topic";
    public static final String GROUP_NAME = "my-group";

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                Utils.printHelp("This example takes 2 parameters (i.e. 10000 sync):%n" +
                    "- records: total number of records to send (required)%n" +
                    "- mode: pass 'sync' to send records synchronously (optional)");
                return;
            }

            int numRecords = Integer.parseInt(args[0]);
            boolean isAsync = args.length == 1 || !args[1].trim().equalsIgnoreCase("sync");

            // stage 1: clean any topics left from previous runs
            Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, -1, TOPIC_NAME);
            CountDownLatch latch = new CountDownLatch(2);

            // stage 2: produce records to topic1
            Producer producerThread = new Producer(
                "producer", KafkaProperties.BOOTSTRAP_SERVERS, TOPIC_NAME, isAsync, null, false, numRecords, -1, latch);
            producerThread.start();

            // stage 3: consume records from topic1
            Consumer consumerThread = new Consumer(
                "consumer", KafkaProperties.BOOTSTRAP_SERVERS, TOPIC_NAME, GROUP_NAME, Optional.empty(), false, numRecords, latch);
            consumerThread.start();

            if (!latch.await(5, TimeUnit.MINUTES)) {
                Utils.printErr("Timeout after 5 minutes waiting for termination");
                producerThread.shutdown();
                consumerThread.shutdown();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
