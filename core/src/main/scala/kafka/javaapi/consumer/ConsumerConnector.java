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
package kafka.javaapi.consumer;

import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.serializer.Decoder;

import java.util.List;
import java.util.Map;

/**
 * @deprecated since 0.11.0.0, this interface will be removed in a future release.
 */
@Deprecated
public interface ConsumerConnector {
    /**
     *  Create a list of MessageStreams of type T for each topic.
     *
     *  @param topicCountMap  a map of (topic, #streams) pair
     *  @param keyDecoder a decoder that decodes the message key
     *  @param valueDecoder a decoder that decodes the message itself
     *  @return a map of (topic, list of  KafkaStream) pairs.
     *          The number of items in the list is #streams. Each stream supports
     *          an iterator over message/metadata pairs.
     */
    public <K, V> Map<String, List<KafkaStream<K, V>>>
        createMessageStreams(Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder);

    public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap);

    /**
     *  Create a list of MessageAndTopicStreams containing messages of type T.
     *
     *  @param topicFilter a TopicFilter that specifies which topics to
     *                    subscribe to (encapsulates a whitelist or a blacklist).
     *  @param numStreams the number of message streams to return.
     *  @param keyDecoder a decoder that decodes the message key
     *  @param valueDecoder a decoder that decodes the message itself
     *  @return a list of KafkaStream. Each stream supports an
     *          iterator over its MessageAndMetadata elements.
     */
    public <K, V> List<KafkaStream<K, V>>
        createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder);

    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams);

    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter);

    /**
     *  Commit the offsets of all broker partitions connected by this connector.
     */
    public void commitOffsets();

    public void commitOffsets(boolean retryOnFailure);

    /**
     *  Commit offsets using the provided offsets map
     *
     *  @param offsetsToCommit a map containing the offset to commit for each partition.
     *  @param retryOnFailure enable retries on the offset commit if it fails.
     */
    public void commitOffsets(Map<TopicAndPartition, OffsetAndMetadata> offsetsToCommit, boolean retryOnFailure);

    /**
     * Wire in a consumer rebalance listener to be executed when consumer rebalance occurs.
     * @param listener The consumer rebalance listener to wire in
     */
    public void setConsumerRebalanceListener(ConsumerRebalanceListener listener);

    /**
     *  Shut down the connector
     */
    public void shutdown();
}
