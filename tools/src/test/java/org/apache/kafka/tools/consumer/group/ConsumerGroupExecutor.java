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

package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.RangeAssignor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ConsumerGroupExecutor implements AutoCloseable {
    final int numThreads;
    final ExecutorService executor;
    final List<ConsumerRunnable> consumers = new ArrayList<>();

    private ConsumerGroupExecutor(
            String broker,
            int numConsumers,
            String groupId,
            String groupProtocol,
            String topic,
            String assignmentStrategy,
            Optional<String> remoteAssignor,
            Optional<Properties> customPropsOpt,
            boolean syncCommit
    ) {
        this.numThreads = numConsumers;
        this.executor = Executors.newFixedThreadPool(numThreads);
        IntStream.rangeClosed(1, numConsumers).forEach(i -> {
            ConsumerRunnable th = new ConsumerRunnable(
                    broker,
                    groupId,
                    groupProtocol,
                    topic,
                    assignmentStrategy,
                    remoteAssignor,
                    customPropsOpt,
                    syncCommit
            );
            th.configure();
            submit(th);
        });
    }

    public static ConsumerGroupExecutor buildConsumerGroup(String broker,
                                                           int numConsumers,
                                                           String groupId,
                                                           String topic,
                                                           String groupProtocol,
                                                           Optional<String> remoteAssignor,
                                                           Optional<Properties> customPropsOpt,
                                                           boolean syncCommit) {
        return new ConsumerGroupExecutor(
                broker,
                numConsumers,
                groupId,
                groupProtocol,
                topic,
                RangeAssignor.class.getName(),
                remoteAssignor,
                customPropsOpt,
                syncCommit
        );
    }

    public static ConsumerGroupExecutor buildClassicGroup(String broker,
                                                          int numConsumers,
                                                          String groupId,
                                                          String topic,
                                                          String assignmentStrategy,
                                                          Optional<Properties> customPropsOpt,
                                                          boolean syncCommit) {
        return new ConsumerGroupExecutor(
                broker,
                numConsumers,
                groupId,
                GroupProtocol.CLASSIC.name,
                topic,
                assignmentStrategy,
                Optional.empty(),
                customPropsOpt,
                syncCommit
        );
    }

    void submit(ConsumerRunnable consumerThread) {
        consumers.add(consumerThread);
        executor.submit(consumerThread);
    }

    @Override
    public void close() throws Exception {
        consumers.forEach(ConsumerRunnable::shutdown);
        executor.shutdown();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

