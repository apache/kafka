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
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages the fetching process with the brokers.
 * <p>
 * Thread-safety:
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 * <ul>
 *     <li>If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler),
 *     all access to that state must be synchronized on the Fetcher instance.</li>
 *     <li>If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState),
 *     it is assumed that all access to that state is synchronized on the coordinator instance by
 *     the caller.</li>
 *     <li>At most one request is pending for each node at any time. Nodes with pending requests are
 *     tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 *     updated while processing responses on one thread are visible while creating the subsequent request
 *     on a different thread.</li>
 * </ul>
 */
public class Fetcher<K, V> extends AbstractFetch<K, V> implements Closeable {

    private final Logger log;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public Fetcher(ConsumerNetworkClient client,
                   ConsumerMetadata metadata,
                   SubscriptionState subscriptions,
                   FetchConfig<K, V> fetchConfig,
                   FetchState<K, V> fetchState,
                   Metrics metrics,
                   FetcherMetricsRegistry metricsRegistry,
                   Time time) {
        super(client, metadata, subscriptions, fetchConfig, fetchState, metrics, metricsRegistry, time);
        this.log = fetchState.logContext().logger(Fetcher.class);
    }

    public static Sensor throttleTimeSensor(final Metrics metrics, final FetcherMetricsRegistry metricsRegistry) {
        Sensor fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), new Avg());
        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), new Max());
        return fetchThrottleTimeSensor;
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     */
    public synchronized int sendFetches() {
        // Update metrics in case there was an assignment change
        sensors.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final RequestFuture<ClientResponse> future = sendFetchRequestToNode(data, fetchTarget);

            // We add the node to the set of nodes with pending fetch requests before adding the
            // listener because the future may have been fulfilled on another thread (e.g. during a
            // disconnection being handled by the heartbeat thread) which will mean the listener
            // will be invoked synchronously.
            fetchState.addPendingRequest(fetchTarget);

            future.addListener(new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse resp) {
                    synchronized (Fetcher.this) {
                        try {
                            handleSuccessfulFetchRequest(fetchTarget, resp, data);
                        } finally {
                            fetchState.removePendingRequest(fetchTarget);
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (Fetcher.this) {
                        try {
                            handleFailedFetchRequest(fetchTarget, e);
                        } finally {
                            fetchState.removePendingRequest(fetchTarget);
                        }
                    }
                }
            });
        }

        return fetchRequestMap.size();
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions
     *
     * @param assignedPartitions  newly assigned {@link TopicPartition}
     */
    public void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions) {
        fetchState.clearBufferedDataForUnassignedPartitions(assignedPartitions);
    }

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    public void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics) {
        final Set<TopicPartition> currentTopicPartitions = assignedTopicPartitions(assignedTopics);
        fetchState.clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
    }

    public void close(final Timer timer) {
        if (!isClosed.compareAndSet(false, true)) {
            log.info("Fetcher {} is already closed.", this);
            return;
        }

        // Shared states (e.g. sessionHandlers) could be accessed by multiple threads (such as heartbeat thread), hence,
        // it is necessary to acquire a lock on the fetcher instance before modifying the states.
        synchronized (Fetcher.this) {
            // we do not need to re-enable wakeups since we are closing already
            client.disableWakeups();
            maybeCloseFetchSessions(timer);
            fetchState.close();
        }
    }

    @Override
    public void close() {
        close(time.timer(0));
    }

}
