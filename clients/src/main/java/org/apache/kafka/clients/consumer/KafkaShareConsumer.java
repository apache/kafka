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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ShareConsumerDelegate;
import org.apache.kafka.clients.consumer.internals.ShareConsumerDelegateCreator;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * A client that consumes records from a Kafka cluster using a share group.
 * <p>
 *     <em>This is an early access feature under development which is introduced by KIP-932.
 *     It is not suitable for production use until it is fully implemented and released.</em>
 *
 * <h3>Cross-Version Compatibility</h3>
 * This client can communicate with brokers that are a version that supports share groups. You will receive an
 * {@link org.apache.kafka.common.errors.UnsupportedVersionException} when invoking an API that is not
 * available on the running broker version.
 *
 * <h3><a name="sharegroups">Share Groups and Topic Subscriptions</a></h3>
 * Kafka uses the concept of <i>share groups</i> to allow a pool of consumers to cooperate on the work of
 * consuming and processing records. All consumer instances sharing the same {@code group.id} will be part of
 * the same share group.
 * <p>
 * Each consumer in a group can dynamically set the list of topics it wants to subscribe to using the
 * {@link #subscribe(Collection)} method. Kafka will deliver each message in the subscribed topics to one
 * consumer in the share group. Unlike consumer groups, share groups balance the partitions between all
 * members of the share group permitting multiple consumers to consume from the same partitions. This gives
 * more flexible sharing of records than a consumer group, at the expense of record ordering.
 * <p>
 * Membership in a share group is maintained dynamically: if a consumer fails, the partitions assigned to
 * it will be reassigned to other consumers in the same group. Similarly, if a new consumer joins the group,
 * the partition assignment is re-evaluated and partitions can be moved from existing consumers to the new one.
 * This is known as <i>rebalancing</i> the group and is discussed in more detail <a href="#failures">below</a>.
 * Group rebalancing is also used when new partitions are added to one of the subscribed topics. The group will
 * automatically detect the new partitions through periodic metadata refreshes and assign them to the members of the group.
 * <p>
 * Conceptually, you can think of a share group as a single logical subscriber made up of multiple consumers.
 * In fact, in other messaging systems, a share group is roughly equivalent to a <em>durable shared subscription</em>.
 * You can have multiple share groups and consumer groups independently consuming from the same topics.
 *
 * <h3><a name="failures">Detecting Consumer Failures</a></h3>
 * After subscribing to a set of topics, the consumer will automatically join the group when {@link #poll(Duration)} is
 * invoked. This method is designed to ensure consumer liveness. As long as you continue to call poll, the consumer
 * will stay in the group and continue to receive records from the partitions it was assigned. Under the covers,
 * the consumer sends periodic heartbeats to the broker. If the consumer crashes or is unable to send heartbeats for
 * the duration of the share group's session time-out, then the consumer will be considered dead and its partitions
 * will be reassigned.
 * <p>
 * It is also possible that the consumer could encounter a "livelock" situation where it is continuing to send heartbeats
 * in the background, but no progress is being made. To prevent the consumer from holding onto its partitions
 * indefinitely in this case, we provide a liveness detection mechanism using the {@code max.poll.interval.ms} setting.
 * If you don't call poll at least as frequently as this, the client will proactively leave the share group.
 * So to stay in the group, you must continue to call poll.
 *
 * <h3>Record Delivery and Acknowledgement</h3>
 * When a consumer in a share-group fetches records using {@link #poll(Duration)}, it receives available records from any
 * of the topic-partitions that match its subscriptions. Records are acquired for delivery to this consumer with a
 * time-limited acquisition lock. While a record is acquired, it is not available for another consumer. By default,
 * the lock duration is 30 seconds, but it can also be controlled using the group {@code group.share.record.lock.duration.ms}
 * configuration parameter. The idea is that the lock is automatically released once the lock duration has elapsed, and
 * then the record is available to be given to another consumer. The consumer which holds the lock can deal with it in
 * the following ways:
 * <ul>
 *     <li>The consumer can acknowledge successful processing of the record</li>
 *     <li>The consumer can release the record, which makes the record available for another delivery attempt</li>
 *     <li>The consumer can reject the record, which indicates that the record is unprocessable and does not make
 *     the record available for another delivery attempt</li>
 *     <li>The consumer can do nothing, in which case the lock is automatically released when the lock duration has elapsed</li>
 * </ul>
 * The cluster limits the number of records acquired for consumers for each topic-partition in a share group. Once the limit
 * is reached, fetching records will temporarily yield no further records until the number of acquired records reduces,
 * as naturally happens when the locks time out. This limit is controlled by the broker configuration property
 * {@code group.share.record.lock.partition.limit}. By limiting the duration of the acquisition lock and automatically
 * releasing the locks, the broker ensures delivery progresses even in the presence of consumer failures.
 * <p>
 * The consumer can choose to use implicit or explicit acknowledgement of the records it processes.
 * <p>If the application calls {@link #acknowledge(ConsumerRecord, AcknowledgeType)} for any record in the batch,
 * it is using <em>explicit acknowledgement</em>. In this case:
 * <ul>
 *     <li>The application calls {@link #commitSync()} or {@link #commitAsync()} which commits the acknowledgements to Kafka.
 *     If any records in the batch were not acknowledged, they remain acquired and will be presented to the application
 *     in response to a future poll.</li>
 *     <li>The application calls {@link #poll(Duration)} without committing first, which commits the acknowledgements to
 *     Kafka asynchronously. In this case, no exception is thrown by a failure to commit the acknowledgement.
 *     If any records in the batch were not acknowledged, they remain acquired and will be presented to the application
 *     in response to a future poll.</li>
 *     <li>The application calls {@link #close()} which attempts to commit any pending acknowledgements and
 *     releases any remaining acquired records.</li>
 * </ul>
 * If the application does not call {@link #acknowledge(ConsumerRecord, AcknowledgeType)} for any record in the batch,
 * it is using <em>implicit acknowledgement</em>. In this case:
 * <ul>
 *     <li>The application calls {@link #commitSync()} or {@link #commitAsync()} which implicitly acknowledges all of
 *     the delivered records as processed successfully and commits the acknowledgements to Kafka.</li>
 *     <li>The application calls {@link #poll(Duration)} without committing, which also implicitly acknowledges all of
 *     the delivered records and commits the acknowledgements to Kafka asynchronously. In this case, no exception is
 *     thrown by a failure to commit the acknowledgements.</li>
 *     <li>The application calls {@link #close()}  which releases any acquired records without acknowledgement.</li>
 * </ul>
 * <p>
 * The consumer guarantees that the records returned in the {@code ConsumerRecords} object for a specific topic-partition
 * are in order of increasing offset. For each topic-partition, Kafka guarantees that acknowledgements for the records
 * in a batch are performed atomically. This makes error handling significantly more straightforward because there can be
 * one error code per partition.
 *
 * <h3>Usage Examples</h3>
 * The share consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 *
 * <h4>Acknowledging a batch of records (implicit acknowledgement)</h4>
 * This example demonstrates implicit acknowledgement using {@link #poll(Duration)} to acknowledge the records which
 * were delivered in the previous poll. All the records delivered are implicitly marked as successfully consumed and
 * acknowledged synchronously with Kafka as the consumer fetches more records.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *             doProcessing(record);
 *         }
 *     }
 * </pre>
 *
 * Alternatively, you can use {@link #commitSync()} or {@link #commitAsync()} to commit the acknowledgements, but this is
 * slightly less efficient because there is an additional request sent to Kafka.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *             doProcessing(record);
 *         }
 *         consumer.commitSync();
 *     }
 * </pre>
 *
 * <h4>Per-record acknowledgement (explicit acknowledgement)</h4>
 * This example demonstrates using different acknowledgement types depending on the outcome of processing the records.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             try {
 *                 doProcessing(record);
 *                 consumer.acknowledge(record, AcknowledgeType.ACCEPT);
 *             } catch (Exception e) {
 *                 consumer.acknowledge(record, AcknowledgeType.REJECT);
 *             }
 *         }
 *         consumer.commitSync();
 *     }
 * </pre>
 *
 * Each record processed is separately acknowledged using a call to {@link #acknowledge(ConsumerRecord, AcknowledgeType)}.
 * The {@link AcknowledgeType} argument indicates whether the record was processed successfully or not. In this case,
 * the bad records are rejected meaning that they’re not eligible for further delivery attempts. For a permanent error
 * such as a semantic error, this is appropriate. For a transient error which might not affect a subsequent processing
 * attempt, {@link AcknowledgeType#RELEASE} is more appropriate because the record remains eligible for further delivery attempts.
 * <p>
 * The calls to {@link #acknowledge(ConsumerRecord, AcknowledgeType)} are simply updating local information in the consumer.
 * It is only once {@link #commitSync()} is called that the acknowledgements are committed by sending the new state
 * information to Kafka.
 *
 * <h4>Per-record acknowledgement, ending processing of the batch on an error (explicit acknowledgement)</h4>
 * This example demonstrates ending processing of a batch of records on the first error.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             try {
 *                 doProcessing(record);
 *                 consumer.acknowledge(record, AcknowledgeType.ACCEPT);
 *             } catch (Exception e) {
 *                 consumer.acknowledge(record, AcknowledgeType.REJECT);
 *                 break;
 *             }
 *         }
 *         consumer.commitSync();
 *     }
 * </pre>
 * There are the following cases in this example:
 * <ol>
 *     <li>The batch contains no records, in which case the application just polls again. The call to {@link #commitSync()}
 *     just does nothing because the batch was empty.</li>
 *     <li>All of the records in the batch are processed successfully. The calls to {@link #acknowledge(ConsumerRecord, AcknowledgeType)}
 *     specifying {@code AcknowledgeType.ACCEPT} mark all records in the batch as successfully processed.</li>
 *     <li>One of the records encounters an exception. The call to {@link #acknowledge(ConsumerRecord, AcknowledgeType)} specifying
 *     {@code AcknowledgeType.REJECT} rejects that record. Earlier records in the batch have already been marked as successfully
 *     processed. The call to {@link #commitSync()} commits the acknowledgements, but the records after the failed record
 *     remain acquired as part of the same delivery attempt and will be presented to the application in response to another poll.</li>
 * </ol>
 *
 * <h3>Reading Transactional Records</h3>
 * The way that share groups handle transactional records is controlled by the {@code group.share.isolation.level}</code>
 * configuration property. In a share group, the isolation level applies to the entire share group, not just individual
 * consumers.
 * <p>
 * In <code>read_uncommitted</code> isolation level, the share group consumes all non-transactional and transactional
 * records. The consumption is bounded by the high-water mark.
 * <p>
 * In <code>read_committed</code> isolation level (not yet supported), the share group only consumes non-transactional
 * records and committed transactional records. The set of records which are eligible to become in-flight records are
 * non-transactional records and committed transactional records only. The consumption is bounded by the last stable
 * offset, so an open transaction blocks the progress of the share group with read_committed isolation level.
 *
 * <h3><a name="multithreaded">Multithreaded Processing</a></h3>
 * The consumer is NOT thread-safe. It is the responsibility of the user to ensure that multithreaded access
 * is properly synchronized. Unsynchronized access will result in {@link java.util.ConcurrentModificationException}.
 * <p>
 * The only exception to this rule is {@link #wakeup()} which can safely be used from an external thread to
 * interrupt an active operation. In this case, a {@link org.apache.kafka.common.errors.WakeupException} will be
 * thrown from the thread blocking on the operation. This can be used to shut down the consumer from another thread.
 * The following snippet shows the typical pattern:
 *
 * <pre>
 * public class KafkaShareConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaShareConsumer consumer;
 *
 *     public KafkaShareConsumerRunner(KafkaShareConsumer consumer) {
 *       this.consumer = consumer;
 *     }
 *
 *     {@literal}@Override
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *                 // Handle new records
 *             }
 *         } catch (WakeupException e) {
 *             // Ignore exception if closing
 *             if (!closed.get()) throw e;
 *         } finally {
 *             consumer.close();
 *         }
 *     }
 *
 *     // Shutdown hook which can be called from a separate thread
 *     public void shutdown() {
 *         closed.set(true);
 *         consumer.wakeup();
 *     }
 * }
 * </pre>
 *
 * Then in a separate thread, the consumer can be shutdown by setting the closed flag and waking up the consumer.
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * Note that while it is possible to use thread interrupts instead of {@link #wakeup()} to abort a blocking operation
 * (in which case, {@link InterruptException} will be raised), we discourage their use since they may cause a clean
 * shutdown of the consumer to be aborted. Interrupts are mainly supported for those cases where using {@link #wakeup()}
 * is impossible, such as when a consumer thread is managed by code that is unaware of the Kafka client.
 * <p>
 * We have intentionally avoided implementing a particular threading model for processing. Various options for
 * multithreaded processing are possible, of which the most straightforward is to dedicate a thread to each consumer.
 */
@InterfaceStability.Evolving
public class KafkaShareConsumer<K, V> implements ShareConsumer<K, V> {

    private static final ShareConsumerDelegateCreator CREATOR = new ShareConsumerDelegateCreator();

    private final ShareConsumerDelegate<K, V> delegate;

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

    KafkaShareConsumer(ConsumerConfig config,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    KafkaShareConsumer(final LogContext logContext,
                       final String clientId,
                       final String groupId,
                       final ConsumerConfig config,
                       final Deserializer<K> keyDeserializer,
                       final Deserializer<V> valueDeserializer,
                       final Time time,
                       final KafkaClient client,
                       final SubscriptionState subscriptions,
                       final ConsumerMetadata metadata) {
        delegate = CREATOR.create(
                logContext, clientId, groupId, config, keyDeserializer, valueDeserializer,
                time, client, subscriptions, metadata);
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
     * @throws IllegalArgumentException if topics is null or contains null or empty elements
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
     *             topics or to the share group. See the exception for more details
     * @throws IllegalArgumentException if the timeout value is negative
     * @throws IllegalStateException if the consumer is not subscribed to any topics
     * @throws ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the calling thread is interrupted before or while this method is called
     * @throws KafkaException for any other unrecoverable errors
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
     * @param record The record to acknowledge
     *
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
     * @param record The record to acknowledge
     * @param type The acknowledgement type which indicates whether it was processed successfully
     *
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
     *
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms}
     * expires.
     *
     * @return A map of the results for each topic-partition for which delivery was acknowledged.
     *         If the acknowledgement failed for a topic-partition, an exception is present.
     *
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted while blocked
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
     * @throws IllegalArgumentException if the {@code timeout} is negative
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted while blocked
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
     * Sets the acknowledgement commit callback which can be used to handle acknowledgement completion.
     *
     * @param callback The acknowledgement commit callback
     */
    @Override
    public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
        delegate.setAcknowledgementCommitCallback(callback);
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
     * @throws IllegalArgumentException if the {@code timeout} is negative
     * @throws IllegalStateException if telemetry is not enabled
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws KafkaException if an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        consumer client is otherwise unusable
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
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted before or while this method is called
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
     * @throws IllegalArgumentException if the {@code timeout} is negative
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted before or while this method is called
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

    // Functions below are for testing only
    String clientId() {
        return delegate.clientId();
    }

    Metrics metricsRegistry() {
        return delegate.metricsRegistry();
    }

    KafkaShareConsumerMetrics kafkaShareConsumerMetrics() {
        return delegate.kafkaShareConsumerMetrics();
    }
}
