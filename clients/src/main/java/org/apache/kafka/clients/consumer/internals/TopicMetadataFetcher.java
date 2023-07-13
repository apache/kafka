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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link TopicMetadataFetcher} is responsible for fetching the {@link PartitionInfo} for a given set of topics.
 * All methods are blocking up to the {@link Timer timeout} provided.
 */
public class TopicMetadataFetcher {

    private final Logger log;
    private final ConsumerNetworkClient client;
    private final ExponentialBackoff retryBackoff;

    public TopicMetadataFetcher(LogContext logContext, ConsumerNetworkClient client, long retryBackoffMs, long retryBackoffMaxMs) {
        this.log = logContext.logger(getClass());
        this.client = client;
        this.retryBackoff = new ExponentialBackoff(retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
    }

    /**
     * Fetches the {@link PartitionInfo partition information} for the given topic in the cluster, or {@code null}.
     *
     * @param timer Timer bounding how long this method can block
     * @return The {@link List list} of {@link PartitionInfo partition information}, or {@code null} if the topic is
     * unknown
     */
    public List<PartitionInfo> getTopicMetadata(String topic, boolean allowAutoTopicCreation, Timer timer) {
        MetadataRequest.Builder request = new MetadataRequest.Builder(Collections.singletonList(topic), allowAutoTopicCreation);
        Map<String, List<PartitionInfo>> topicMetadata = getTopicMetadata(request, timer);
        return topicMetadata.get(topic);
    }

    /**
     * Fetches the {@link PartitionInfo partition information} for all topics in the cluster.
     *
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their {@link PartitionInfo partition information}
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
        MetadataRequest.Builder request = MetadataRequest.Builder.allTopics();
        return getTopicMetadata(request, timer);
    }

    /**
     * Get metadata for all topics present in Kafka cluster.
     *
     * @param request The MetadataRequest to send
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    private Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer) {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics() && request.emptyTopicList())
            return Collections.emptyMap();

        long attempts = 0L;
        do {
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            client.poll(future, timer);

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            if (future.succeeded()) {
                MetadataResponse response = (MetadataResponse) future.value().responseBody();
                Cluster cluster = response.buildCluster();

                Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
                if (!unauthorizedTopics.isEmpty())
                    throw new TopicAuthorizationException(unauthorizedTopics);

                boolean shouldRetry = false;
                Map<String, Errors> errors = response.errors();
                if (!errors.isEmpty()) {
                    // if there were errors, we need to check whether they were fatal or whether
                    // we should just retry

                    log.debug("Topic metadata fetch included errors: {}", errors);

                    for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                        String topic = errorEntry.getKey();
                        Errors error = errorEntry.getValue();

                        if (error == Errors.INVALID_TOPIC_EXCEPTION)
                            throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                            // if a requested topic is unknown, we just continue and let it be absent
                            // in the returned map
                            continue;
                        else if (error.exception() instanceof RetriableException)
                            shouldRetry = true;
                        else
                            throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                                    error.exception());
                    }
                }

                if (!shouldRetry) {
                    HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
                    for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            timer.sleep(retryBackoff.backoff(attempts++));
        } while (timer.notExpired());

        throw new TimeoutException("Timeout expired while fetching topic metadata");
    }

    /**
     * Send Metadata Request to the least loaded node in Kafka cluster asynchronously
     * @return A future that indicates result of sent metadata request
     */
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest.Builder request) {
        final Node node = client.leastLoadedNode();
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            return client.send(node, request);
    }

}
