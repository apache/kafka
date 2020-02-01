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

import java.util.ArrayList;
import java.util.List;

public class KafkaExactlyOnceDemo {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 4) {
            throw new IllegalArgumentException("Should accept 4 parameters: [mode], " +
                "[number of partitions], [number of instances], [number of records]");
        }

        String mode = args[0];
        int numPartitions = Integer.valueOf(args[1]);
        int numInstances = Integer.valueOf(args[2]);
        int numRecords = Integer.valueOf(args[3]);

        // Pre-populate records
        Producer producerThread = new Producer(INPUT_TOPIC, true, null, numRecords);
        producerThread.start();

        synchronized (producerThread.getClass()) {
            while (producerThread.isAlive()) {
                producerThread.getClass().wait();
            }
        }

        List<ExactlyOnceMessageProcessor> eosProcessors = new ArrayList<>(numInstances);
        for (int instanceIdx = 0; instanceIdx < numInstances; instanceIdx++) {
            ExactlyOnceMessageProcessor messageProcessor = new ExactlyOnceMessageProcessor(mode,
                INPUT_TOPIC, OUTPUT_TOPIC, numPartitions, numInstances, instanceIdx);
            eosProcessors.add(messageProcessor);
            messageProcessor.start();
        }

    }

}
