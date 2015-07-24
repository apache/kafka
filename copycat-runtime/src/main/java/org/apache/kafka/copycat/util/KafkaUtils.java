/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.util;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.copycat.connector.TopicPartition;
import scala.collection.Seq;
import scala.collection.mutable.Map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static scala.collection.JavaConversions.*;

/**
 * Some utilities for working with Kafka that aren't included with Kafka itself.
 */
public class KafkaUtils {

    public static List<TopicPartition> getTopicPartitions(ZkClient zkClient, String... topics) {
        return getTopicPartitions(zkClient, Arrays.asList(topics));
    }

    public static List<TopicPartition> getTopicPartitions(ZkClient zkClient, List<String> topics) {
        Seq<String> scalaTopics = asScalaIterable(topics).toSeq();
        List<TopicPartition> result = new ArrayList<TopicPartition>();
        Map<String, Seq<Object>> partitionsByTopic
                = ZkUtils.getPartitionsForTopics(zkClient, scalaTopics);
        for (java.util.Map.Entry<String, Seq<Object>> entry : asJavaMap(partitionsByTopic).entrySet()) {
            for (Object partition : asJavaIterable(entry.getValue())) {
                result.add(new TopicPartition(entry.getKey(), (Integer) partition));
            }
        }
        return result;
    }
}
