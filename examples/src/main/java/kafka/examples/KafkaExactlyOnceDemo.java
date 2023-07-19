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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This example can be decomposed into the following stages:
 * <p>
 * 1. Clean any topics left from previous runs.
 * 2. Set up a producer thread to pre-populate a set of records with even number keys into the input topic.
 *    The demo will block for the record generation to finish, so the producer is synchronous.
 * 3. Set up the transactional instances in separate threads, each one executing a read-process-write loop
 *    (See {@link ExactlyOnceMessageProcessor}). Each EOS instance will drain all records from either given
 *    partitions or auto assigned partitions by actively comparing log end offset with committed offset.
 *    Each record will be processed exactly-once with strong partition level ordering guarantee.
 *    The demo will block until all records are processed and written to the output topic.
 * 4. Create a read_committed consumer thread to verify we have all records in the output topic,
 *    and record ordering at the partition level is maintained.
 *    The demo will block for the consumption of all committed records, with transactional guarantee.
 * <p>
 * Broker version must be >= 2.5.0 in order to run, otherwise the example will throw
 * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
 * <p>
 * If you are using IntelliJ IDEA, the above arguments should be put in `Modify Run Configuration - Program Arguments`.
 * You can also set an output log file in `Modify Run Configuration - Modify options - Save console output to file` to
 * record all the log output together.
 */
public class KafkaExactlyOnceDemo {
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    public static final String GROUP_NAME = "check-group";

    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                Utils.printHelp("This example takes 3 parameters (i.e. 6 3 10000):%n" +
                    "- partition: number of partitions for input and output topics (required)%n" +
                    "- instances: number of application instances (required)%n" +
                    "- records: total number of records (required)");
                return;
            }

            int numPartitions = Integer.parseInt(args[0]);
            int numInstances = Integer.parseInt(args[1]);
            int numRecords = Integer.parseInt(args[2]);

            // stage 1: clean any topics left from previous runs
            Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, numPartitions, INPUT_TOPIC, OUTPUT_TOPIC);

            // stage 2: send demo records to the input-topic
            CountDownLatch producerLatch = new CountDownLatch(1);
            Producer producerThread = new Producer(
                    "producer",
                    KafkaProperties.BOOTSTRAP_SERVERS,
                    INPUT_TOPIC,
                    false,
                    null,
                    true,
                    numRecords,
                    -1,
                    producerLatch);
            producerThread.start();
            if (!producerLatch.await(2, TimeUnit.MINUTES)) {
                Utils.printErr("Timeout after 2 minutes waiting for data load");
                producerThread.shutdown();
                return;
            }

            // stage 3: read from input-topic, process once and write to the output-topic
            CountDownLatch processorsLatch = new CountDownLatch(numInstances);
            List<ExactlyOnceMessageProcessor> processors = IntStream.range(0, numInstances)
                .mapToObj(id -> new ExactlyOnceMessageProcessor(
                        "processor-" + id,
                        KafkaProperties.BOOTSTRAP_SERVERS,
                        INPUT_TOPIC,
                        OUTPUT_TOPIC,
                        processorsLatch))
                .collect(Collectors.toList());
            processors.forEach(ExactlyOnceMessageProcessor::start);
            if (!processorsLatch.await(2, TimeUnit.MINUTES)) {
                Utils.printErr("Timeout after 2 minutes waiting for record copy");
                processors.forEach(ExactlyOnceMessageProcessor::shutdown);
                return;
            }

            // stage 4: check consuming records from the output-topic
            CountDownLatch consumerLatch = new CountDownLatch(1);
            Consumer consumerThread = new Consumer(
                    "consumer",
                    KafkaProperties.BOOTSTRAP_SERVERS,
                    OUTPUT_TOPIC,
                    GROUP_NAME,
                    Optional.empty(),
                    true,
                    numRecords,
                    consumerLatch);
            consumerThread.start();
            if (!consumerLatch.await(2, TimeUnit.MINUTES)) {
                Utils.printErr("Timeout after 2 minutes waiting for output read");
                consumerThread.shutdown();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
