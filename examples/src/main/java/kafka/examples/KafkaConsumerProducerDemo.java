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

import org.apache.kafka.common.utils.Exit;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        ExecutorService threads = Executors.newFixedThreadPool(2);

        Producer producerTask = new Producer(KafkaProperties.TOPIC, isAsync, null, false, 10000, -1);
        Consumer consumerTask = new Consumer(KafkaProperties.TOPIC, "DemoConsumer", Optional.empty(), false, 10000, KafkaProperties.NON_TRANSACTIONAL);

        try {
            CompletableFuture.allOf(
                    CompletableFuture.runAsync(producerTask, threads),
                    CompletableFuture.runAsync(consumerTask, threads)
            ).get(5, TimeUnit.MINUTES);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (java.util.concurrent.TimeoutException e) {
            System.out.println("Timeout after 5 minutes waiting for demo producer and consumer to finish");
            Exit.exit(1);
        }

        threads.shutdownNow();
        System.out.println("All finished!");
    }
}
