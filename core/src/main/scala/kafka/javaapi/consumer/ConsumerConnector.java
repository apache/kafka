/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.javaapi.consumer;

import kafka.consumer.KafkaMessageStream;
import kafka.message.Message;
import kafka.serializer.Decoder;

import java.util.List;
import java.util.Map;

public interface ConsumerConnector {
    /**
     *  Create a list of MessageStreams of type T for each topic.
     *
     *  @param topicCountMap  a map of (topic, #streams) pair
     *  @param decoder a decoder that converts from Message to T
     *  @return a map of (topic, list of  KafkaMessageStream) pair. The number of items in the
     *          list is #streams. Each KafkaMessageStream supports an iterator of messages.
     */
    public <T> Map<String, List<KafkaMessageStream<T>>> createMessageStreams(
            Map<String, Integer> topicCountMap, Decoder<T> decoder);
    public Map<String, List<KafkaMessageStream<Message>>> createMessageStreams(
            Map<String, Integer> topicCountMap);

    /**
     *  Commit the offsets of all broker partitions connected by this connector.
     */
    public void commitOffsets();

    /**
     *  Shut down the connector
     */
    public void shutdown();
}
