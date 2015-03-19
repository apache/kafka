/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * This class manage the fetching process with the brokers.
 */
public class Fetcher<K, V> {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);
    private static final long EARLIEST_OFFSET_TIMESTAMP = -2L;
    private static final long LATEST_OFFSET_TIMESTAMP = -1L;


    private final KafkaClient client;

    private final Time time;
    private final int minBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final boolean checkCrcs;
    private final long retryBackoffMs;
    private final Metadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final List<PartitionRecords<K, V>> records;
    private final AutoOffsetResetStrategy offsetResetStrategy;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;


    public Fetcher(KafkaClient client,
                   long retryBackoffMs,
                   int minBytes,
                   int maxWaitMs,
                   int fetchSize,
                   boolean checkCrcs,
                   String offsetReset,
                   Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer,
                   Metadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   String metricGrpPrefix,
                   Map<String, String> metricTags,
                   Time time) {

        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.retryBackoffMs = retryBackoffMs;
        this.minBytes = minBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.checkCrcs = checkCrcs;
        this.offsetResetStrategy = AutoOffsetResetStrategy.valueOf(offsetReset);

        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;

        this.records = new LinkedList<PartitionRecords<K, V>>();
        this.sensors = new FetchManagerMetrics(metrics, metricGrpPrefix, metricTags);
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't have one.
     *
     * @param cluster The current cluster metadata
     * @param now The current time
     */
    public void initFetches(Cluster cluster, long now) {
        for (ClientRequest request : createFetchRequests(cluster)) {
            Node node = cluster.nodeById(request.request().destination());
            if (client.ready(node, now)) {
                log.trace("Initiating fetch to node {}: {}", node.id(), request);
                client.send(request);
            }
        }
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * @return The fetched records per partition
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        if (this.subscriptions.partitionAssignmentNeeded()) {
            return Collections.emptyMap();
        } else {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
            for (PartitionRecords<K, V> part : this.records) {
                Long consumed = subscriptions.consumed(part.partition);
                if (this.subscriptions.assignedPartitions().contains(part.partition)
                    && (consumed == null || part.fetchOffset == consumed)) {
                    List<ConsumerRecord<K, V>> records = drained.get(part.partition);
                    if (records == null) {
                        records = part.records;
                        drained.put(part.partition, records);
                    } else {
                        records.addAll(part.records);
                    }
                    subscriptions.consumed(part.partition, part.records.get(part.records.size() - 1).offset() + 1);
                } else {
                    // these records aren't next in line based on the last consumed position, ignore them
                    // they must be from an obsolete request
                    log.debug("Ignoring fetched records for {} at offset {}", part.partition, part.fetchOffset);
                }
            }
            this.records.clear();
            return drained;
        }
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     *
     * @param partition The given partition that needs reset offset
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     */
    public void resetOffset(TopicPartition partition) {
        long timestamp;
        if (this.offsetResetStrategy == AutoOffsetResetStrategy.EARLIEST)
            timestamp = EARLIEST_OFFSET_TIMESTAMP;
        else if (this.offsetResetStrategy == AutoOffsetResetStrategy.LATEST)
            timestamp = LATEST_OFFSET_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException("No offset is set and no reset policy is defined");

        log.debug("Resetting offset for partition {} to {} offset.", partition, this.offsetResetStrategy.name()
            .toLowerCase());
        this.subscriptions.seek(partition, offsetBefore(partition, timestamp));
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     *
     * @param topicPartition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return The offset of the message that is published before the given timestamp
     */
    public long offsetBefore(TopicPartition topicPartition, long timestamp) {
        log.debug("Fetching offsets for partition {}.", topicPartition);
        Map<TopicPartition, ListOffsetRequest.PartitionData> partitions = new HashMap<TopicPartition, ListOffsetRequest.PartitionData>(1);
        partitions.put(topicPartition, new ListOffsetRequest.PartitionData(timestamp, 1));
        while (true) {
            long now = time.milliseconds();
            PartitionInfo info = metadata.fetch().partition(topicPartition);
            if (info == null) {
                metadata.add(topicPartition.topic());
                log.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", topicPartition);
                awaitMetadataUpdate();
            } else if (info.leader() == null) {
                log.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", topicPartition);
                awaitMetadataUpdate();
            } else if (this.client.ready(info.leader(), now)) {
                Node node = info.leader();
                ListOffsetRequest request = new ListOffsetRequest(-1, partitions);
                RequestSend send = new RequestSend(node.id(),
                    this.client.nextRequestHeader(ApiKeys.LIST_OFFSETS),
                    request.toStruct());
                ClientRequest clientRequest = new ClientRequest(now, true, send, null);
                this.client.send(clientRequest);
                List<ClientResponse> responses = this.client.completeAll(node.id(), now);
                if (responses.isEmpty())
                    throw new IllegalStateException("This should not happen.");
                ClientResponse response = responses.get(responses.size() - 1);
                if (response.wasDisconnected()) {
                    awaitMetadataUpdate();
                } else {
                    ListOffsetResponse lor = new ListOffsetResponse(response.responseBody());
                    short errorCode = lor.responseData().get(topicPartition).errorCode;
                    if (errorCode == Errors.NONE.code()) {
                        List<Long> offsets = lor.responseData().get(topicPartition).offsets;
                        if (offsets.size() != 1)
                            throw new IllegalStateException("This should not happen.");
                        long offset = offsets.get(0);
                        log.debug("Fetched offset {} for partition {}", offset, topicPartition);
                        return offset;
                    } else if (errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                        || errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                        log.warn("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                            topicPartition);
                        awaitMetadataUpdate();
                    } else {
                        // TODO: we should not just throw exceptions but should handle and log it.
                        Errors.forCode(errorCode).maybeThrow();
                    }
                }
            } else {
                log.debug("Leader for partition {} is not ready, retry fetching offsets", topicPartition);
                client.poll(this.retryBackoffMs, now);
            }
        }
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    private List<ClientRequest> createFetchRequests(Cluster cluster) {
        // create the fetch info
        Map<Integer, Map<TopicPartition, FetchRequest.PartitionData>> fetchable = new HashMap<Integer, Map<TopicPartition, FetchRequest.PartitionData>>();
        for (TopicPartition partition : subscriptions.assignedPartitions()) {
            Node node = cluster.leaderFor(partition);
            // if there is a leader and no in-flight requests, issue a new fetch
            if (node != null && this.client.inFlightRequestCount(node.id()) == 0) {
                Map<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node.id());
                if (fetch == null) {
                    fetch = new HashMap<TopicPartition, FetchRequest.PartitionData>();
                    fetchable.put(node.id(), fetch);
                }
                long offset = this.subscriptions.fetched(partition);
                fetch.put(partition, new FetchRequest.PartitionData(offset, this.fetchSize));
            }
        }

        // create the requests
        List<ClientRequest> requests = new ArrayList<ClientRequest>(fetchable.size());
        for (Map.Entry<Integer, Map<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            int nodeId = entry.getKey();
            final FetchRequest fetch = new FetchRequest(this.maxWaitMs, this.minBytes, entry.getValue());
            RequestSend send = new RequestSend(nodeId, this.client.nextRequestHeader(ApiKeys.FETCH), fetch.toStruct());
            RequestCompletionHandler handler = new RequestCompletionHandler() {
                public void onComplete(ClientResponse response) {
                    handleFetchResponse(response, fetch);
                }
            };
            requests.add(new ClientRequest(time.milliseconds(), true, send, handler));
        }
        return requests;
    }

    /**
     * The callback for fetch completion
     */
    private void handleFetchResponse(ClientResponse resp, FetchRequest request) {
        if (resp.wasDisconnected()) {
            int correlation = resp.request().request().header().correlationId();
            log.debug("Cancelled fetch request {} with correlation id {} due to node {} being disconnected",
                resp.request(), correlation, resp.request().request().destination());
        } else {
            int totalBytes = 0;
            int totalCount = 0;
            FetchResponse response = new FetchResponse(resp.responseBody());
            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                FetchResponse.PartitionData partition = entry.getValue();
                if (!subscriptions.assignedPartitions().contains(tp)) {
                    log.debug("Ignoring fetched data for partition {} which is no longer assigned.", tp);
                } else if (partition.errorCode == Errors.NONE.code()) {
                    int bytes = 0;
                    ByteBuffer buffer = partition.recordSet;
                    MemoryRecords records = MemoryRecords.readableRecords(buffer);
                    long fetchOffset = request.fetchData().get(tp).offset;
                    List<ConsumerRecord<K, V>> parsed = new ArrayList<ConsumerRecord<K, V>>();
                    for (LogEntry logEntry : records) {
                        parsed.add(parseRecord(tp, logEntry));
                        bytes += logEntry.size();
                    }
                    if (parsed.size() > 0) {
                        ConsumerRecord<K, V> record = parsed.get(parsed.size() - 1);
                        this.subscriptions.fetched(tp, record.offset() + 1);
                        this.records.add(new PartitionRecords<K, V>(fetchOffset, tp, parsed));
                        this.sensors.recordsFetchLag.record(partition.highWatermark - record.offset());
                    }
                    this.sensors.recordTopicFetchMetrics(tp.topic(), bytes, parsed.size());
                    totalBytes += bytes;
                    totalCount += parsed.size();
                } else if (partition.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                    || partition.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                    this.metadata.requestUpdate();
                } else if (partition.errorCode == Errors.OFFSET_OUT_OF_RANGE.code()) {
                    // TODO: this could be optimized by grouping all out-of-range partitions
                    log.info("Fetch offset {} is out of range, resetting offset", subscriptions.fetched(tp));
                    resetOffset(tp);
                } else if (partition.errorCode == Errors.UNKNOWN.code()) {
                    log.warn("Unknown error fetching data for topic-partition {}", tp);
                } else {
                    throw new IllegalStateException("Unexpected error code " + partition.errorCode + " while fetching data");
                }
            }
            this.sensors.bytesFetched.record(totalBytes);
            this.sensors.recordsFetched.record(totalCount);
        }
        this.sensors.fetchLatency.record(resp.requestLatencyMs());
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        if (this.checkCrcs)
            logEntry.record().ensureValid();

        long offset = logEntry.offset();
        ByteBuffer keyBytes = logEntry.record().key();
        K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), Utils.toArray(keyBytes));
        ByteBuffer valueBytes = logEntry.record().value();
        V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), Utils.toArray(valueBytes));

        return new ConsumerRecord<K, V>(partition.topic(), partition.partition(), offset, key, value);
    }

    /*
     * Request a metadata update and wait until it has occurred
     */
    private void awaitMetadataUpdate() {
        int version = this.metadata.requestUpdate();
        do {
            long now = time.milliseconds();
            this.client.poll(this.retryBackoffMs, now);
        } while (this.metadata.version() == version);
    }

    private static class PartitionRecords<K, V> {
        public long fetchOffset;
        public TopicPartition partition;
        public List<ConsumerRecord<K, V>> records;

        public PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }
    }

    private static enum AutoOffsetResetStrategy {
        LATEST, EARLIEST, NONE
    }

    private class FetchManagerMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor bytesFetched;
        public final Sensor recordsFetched;
        public final Sensor fetchLatency;
        public final Sensor recordsFetchLag;


        public FetchManagerMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-fetch-manager-metrics";

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(new MetricName("fetch-size-avg",
                this.metricGrpName,
                "The average number of bytes fetched per request",
                tags), new Avg());
            this.bytesFetched.add(new MetricName("fetch-size-max",
                this.metricGrpName,
                "The maximum number of bytes fetched per request",
                tags), new Max());
            this.bytesFetched.add(new MetricName("bytes-consumed-rate",
                this.metricGrpName,
                "The average number of bytes consumed per second",
                tags), new Rate());

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(new MetricName("records-per-request-avg",
                this.metricGrpName,
                "The average number of records in each request",
                tags), new Avg());
            this.recordsFetched.add(new MetricName("records-consumed-rate",
                this.metricGrpName,
                "The average number of records consumed per second",
                tags), new Rate());

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(new MetricName("fetch-latency-avg",
                this.metricGrpName,
                "The average time taken for a fetch request.",
                tags), new Avg());
            this.fetchLatency.add(new MetricName("fetch-latency-max",
                this.metricGrpName,
                "The max time taken for any fetch request.",
                tags), new Max());
            this.fetchLatency.add(new MetricName("fetch-rate",
                this.metricGrpName,
                "The number of fetch requests per second.",
                tags), new Rate(new Count()));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(new MetricName("records-lag-max",
                this.metricGrpName,
                "The maximum lag in terms of number of records for any partition in this window",
                tags), new Max());
        }

        public void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null)
                bytesFetched = this.metrics.sensor(name);
            bytesFetched.record(bytes);

            // record records fetched
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null)
                recordsFetched = this.metrics.sensor(name);
            recordsFetched.record(records);
        }
    }
}
