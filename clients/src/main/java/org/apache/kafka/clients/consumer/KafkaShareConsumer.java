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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.ShareConsumerDelegateCreator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.propsToMap;

public class KafkaShareConsumer<K, V> implements ShareConsumer<K, V> {

    private final static ShareConsumerDelegateCreator CREATOR = new ShareConsumerDelegateCreator();

    private final ShareConsumer<K, V> delegate;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    public KafkaShareConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public KafkaShareConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaShareConsumer(Properties properties,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaShareConsumer(Map<String, Object> configs,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    public KafkaShareConsumer(ConsumerConfig config,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection)}, or an empty set if no such call has been made.
     *
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        return delegate.subscription();
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment, if there is one.</b> If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the coordinator will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     * <ul>
     * <li>A member joins or leaves the share group
     * <li>An existing member of the share group is shut down or fails
     * <li>The number of partitions changes for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * </ul>
     *
     * @param topics The list of topics to subscribe to
     *
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws KafkaException for any other unrecoverable errors
     */
    @Override
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)}.
     *
     * @throws KafkaException for any other unrecoverable errors
     */
    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    /**
     * Fetch data for the topics specified using {@link #subscribe(Collection)}. It is an error to not have
     * subscribed to any topics before polling for data.
     *
     * <p>
     * This method returns immediately if there are records available. Otherwise, it will await the passed timeout.
     * If the timeout expires, an empty record set will be returned.
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws InterruptException if the calling thread is interrupted before or while this method is called
     * @throws InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs,
     *             or any new error cases in future versions)
     * @throws IllegalArgumentException if the timeout value is negative
     * @throws IllegalStateException if the consumer is not subscribed to any topics
     * @throws ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return delegate.poll(timeout);
    }

    /**
     * Acknowledge successful delivery of a record returned on the last {@link #poll(Duration)} call.
     * The acknowledgement is committed on the next {@link #commitSync()}, {@link #commitAsync()} or
     * {@link #poll(Duration)} call.
     *
     * <p>
     * Records for each topic-partition must be acknowledged in the order they were returned from
     * {@link #poll(Duration)}. By using this method, the consumer is using
     * <b>explicit acknowledgement</b>.
     *
     * @param record The record to acknowledge
     *
     * @throws IllegalArgumentException if the record being acknowledged doesn't meet the ordering requirement
     * @throws IllegalStateException if the record is not waiting to be acknowledged, or the consumer has already
     *                               used implicit acknowledgement
     */
    @Override
    public void acknowledge(ConsumerRecord<K, V> record) {
        delegate.acknowledge(record);
    }

    /**
     * Acknowledge delivery of a record returned on the last {@link #poll(Duration)} call indicating whether
     * it was processed successfully. The acknowledgement is committed on the next {@link #commitSync()},
     * {@link #commitAsync()} or {@link #poll(Duration)} call. By using this method, the consumer is using
     * <b>explicit acknowledgement</b>.
     *
     * <p>
     * Records for each topic-partition must be acknowledged in the order they were returned from
     * {@link #poll(Duration)}.
     *
     * @param record The record to acknowledge
     * @param type The acknowledge type which indicates whether it was processed successfully
     *
     * @throws IllegalArgumentException if the record being acknowledged doesn't meet the ordering requirement
     * @throws IllegalStateException if the record is not waiting to be acknowledged, or the consumer has already
     *                               used implicit acknowledgement
     */
    @Override
    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        delegate.acknowledge(record, type);
    }

    /**
     * Commit the acknowledgements for the records returned. If the consumer is using explicit acknowledgement,
     * the acknowledgements to commit have been indicated using {@link #acknowledge(ConsumerRecord)} or
     * {@link #acknowledge(ConsumerRecord, AcknowledgeType)}. If the consumer is using implicit acknowledgement,
     * all the records returned by the latest call to {@link #poll(Duration)} are acknowledged.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @return A map of the results for each topic-partition for which delivery was acknowledged.
     *         If the acknowledgement failed for a topic-partition, an exception is present.
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return delegate.commitSync();
    }

    /**
     * Commit the acknowledgements for the records returned. If the consumer is using explicit acknowledgement,
     * the acknowledgements to commit have been indicated using {@link #acknowledge(ConsumerRecord)} or
     * {@link #acknowledge(ConsumerRecord, AcknowledgeType)}. If the consumer is using implicit acknowledgement,
     * all the records returned by the latest call to {@link #poll(Duration)} are acknowledged.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param timeout The maximum amount of time to await completion of the acknowledgement
     *
     * @return A map of the results for each topic-partition for which delivery was acknowledged.
     *         If the acknowledgement failed for a topic-partition, an exception is present.
     *
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
        return delegate.commitSync(timeout);
    }

    /**
     * Commit the acknowledgements for the records returned. If the consumer is using explicit acknowledgement,
     * the acknowledgements to commit have been indicated using {@link #acknowledge(ConsumerRecord)} or
     * {@link #acknowledge(ConsumerRecord, AcknowledgeType)}. If the consumer is using implicit acknowledgement,
     * all the records returned by the latest call to {@link #poll(Duration)} are acknowledged.
     *
     * @throws KafkaException for any other unrecoverable errors
     */
    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    /**
     * Sets the acknowledge commit callback which can be used to handle acknowledgement completion.
     *
     * @param callback The acknowledge commit callback
     */
    @Override
    public void setAcknowledgeCommitCallback(AcknowledgeCommitCallback callback) {
        delegate.setAcknowledgeCommitCallback(callback);
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the consumer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ConsumerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for consumer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     *
     * @return The client's assigned instance id used for metrics collection.
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        consumer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                               is set to `{@code false}`.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * This will commit acknowledgements if possible within the default timeout.
     * See {@link #close(Duration)} for details. Note that {@link #wakeup()} cannot be used to interrupt close.
     *
     * @throws InterruptException If the thread is interrupted before or while this method is called
     * @throws KafkaException for any other error during close
     */
    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete acknowledgements and leave the group.
     * If the consumer is unable to complete acknowledgements and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     *
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws InterruptException If the thread is interrupted before or while this method is called
     * @throws KafkaException for any other error during close
     */
    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    /**
     * Wake up the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link WakeupException}.
     * If no thread is blocking in a method which can throw {@link WakeupException},
     * the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {
        delegate.wakeup();
    }
}
