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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Utils {
    private Utils() {
    }

    public static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

    public static void recreateTopics(String bootstrapServers, List<String> topicNames, int numPartitions) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-" + UUID.randomUUID());
        Admin adminClient = Admin.create(props);
        try {
            deleteTopics(adminClient, topicNames);
            // create topics in a retry loop
            while (true) {
                // use default RF to avoid NOT_ENOUGH_REPLICAS error with minISR>1
                short replicationFactor = -1;
                List<NewTopic> newTopics = topicNames.stream()
                    .map(name -> new NewTopic(name, numPartitions, replicationFactor))
                    .collect(Collectors.toList());
                try {
                    adminClient.createTopics(newTopics).all().get();
                    System.out.printf("Created topics: %s%n", topicNames);
                    break;
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                    System.out.println("Waiting for topics metadata cleanup...");
                    sleep(1_000);
                }
            }
        } catch (Throwable e) {
            adminClient.close();
            throw new RuntimeException("Topic creation failed");
        }
    }

    private static void deleteTopics(Admin adminClient, List<String> topicNames)
        throws InterruptedException, ExecutionException {
        try {
            adminClient.deleteTopics(topicNames).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
            System.err.printf("Topic deletion error: %s%n", e.getCause());
        }
        System.out.printf("Deleted topics: %s%n", topicNames);
    }
}
