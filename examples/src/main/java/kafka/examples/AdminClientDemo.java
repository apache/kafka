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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final String topic = "test";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient client = AdminClient.create(properties);

        DescribeClusterResult clusterResult = client.describeCluster();
        System.out.println("Describe Cluster : cluster ID : " + clusterResult.clusterId().get() + " " +
                "controller : " + clusterResult.controller().get().toString() + " " +
                "nodes : " + clusterResult.nodes().get());

        CreateTopicsResult result = client.createTopics(Arrays.asList(new NewTopic(topic, 1, (short) 1)));
        for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
            System.out.println("Create Topic : " + entry.getKey() + " " +
                    "isCancelled : " + entry.getValue().isCancelled() + " " +
                    "isCompletedExceptionally : " + entry.getValue().isCompletedExceptionally() + " " +
                    "isDone : " + entry.getValue().isDone());
        }

        ListTopicsResult listTopicsResult = client.listTopics();
        for (TopicListing listing : listTopicsResult.listings().get()) {
            System.out.println("List Topic : " + listing.name());
        }

        DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : describeTopicsResult.values().entrySet()) {
            System.out.println("Describe Topics : " + entry.getKey() + " Name : " + entry.getValue().get().name() +
                    " Partitions : " + entry.getValue().get().partitions());
        }

        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList(topic));
        for (Map.Entry<String, KafkaFuture<Void>> entry : deleteTopicsResult.values().entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue().get());
        }
        client.close();
    }
}