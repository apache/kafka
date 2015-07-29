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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Coordinator;
import org.apache.kafka.clients.consumer.internals.DelayedTask;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.min;

/**
 * A Kafka client that consumes records from a Kafka cluster.
 * <p>
 * It will transparently handle the failure of servers in the Kafka cluster, and transparently adapt as partitions of
 * data it subscribes to migrate within the cluster. This client also interacts with the server to allow groups of
 * consumers to load balance consumption using consumer groups (as described below).
 * <p>
 * The consumer maintains TCP connections to the necessary brokers to fetch data for the topics it subscribes to.
 * Failure to close the consumer after use will leak these connections.
 * <p>
 * The consumer is not thread-safe. See <a href="#multithreaded">Multi-threaded Processing</a> for more details.
 *
 * <h3>Offsets and Consumer Position</h3>
 * Kafka maintains a numerical offset for each record in a partition. This offset acts as a kind of unique identifier of
 * a record within that partition, and also denotes the position of the consumer in the partition. That is, a consumer
 * which has position 5 has consumed records with offsets 0 through 4 and will next receive record with offset 5. There
 * are actually two notions of position relevant to the user of the consumer.
 * <p>
 * The {@link #position(TopicPartition) position} of the consumer gives the offset of the next record that will be given
 * out. It will be one larger than the highest offset the consumer has seen in that partition. It automatically advances
 * every time the consumer receives data calls {@link #poll(long)} and receives messages.
 * <p>
 * The {@link #commit(CommitType) committed position} is the last offset that has been saved securely. Should the
 * process fail and restart, this is the offset that it will recover to. The consumer can either automatically commit
 * offsets periodically, or it can choose to control this committed position manually by calling
 * {@link #commit(CommitType) commit}.
 * <p>
 * This distinction gives the consumer control over when a record is considered consumed. It is discussed in further
 * detail below.
 * 
 * <h3>Consumer Groups</h3>
 * 
 * Kafka uses the concept of <i>consumer groups</i> to allow a pool of processes to divide up the work of consuming and
 * processing records. These processes can either be running on the same machine or, as is more likely, they can be
 * distributed over many machines to provide additional scalability and fault tolerance for processing.
 * <p>
 * Each Kafka consumer must specify a consumer group that it belongs to. Kafka will deliver each message in the
 * subscribed topics to one process in each consumer group. This is achieved by balancing the partitions in the topic
 * over the consumer processes in each group. So if there is a topic with four partitions, and a consumer group with two
 * processes, each process would consume from two partitions. This group membership is maintained dynamically: if a
 * process fails the partitions assigned to it will be reassigned to other processes in the same group, and if a new
 * process joins the group, partitions will be moved from existing consumers to this new process.
 * <p>
 * So if two processes subscribe to a topic both specifying different groups they will each get all the records in that
 * topic; if they both specify the same group they will each get about half the records.
 * <p>
 * Conceptually you can think of a consumer group as being a single logical subscriber that happens to be made up of
 * multiple processes. As a multi-subscriber system, Kafka naturally supports having any number of consumer groups for a
 * given topic without duplicating data (additional consumers are actually quite cheap).
 * <p>
 * This is a slight generalization of the functionality that is common in messaging systems. To get semantics similar to
 * a queue in a traditional messaging system all processes would be part of a single consumer group and hence record
 * delivery would be balanced over the group like with a queue. Unlike a traditional messaging system, though, you can
 * have multiple such groups. To get semantics similar to pub-sub in a traditional messaging system each process would
 * have it's own consumer group, so each process would subscribe to all the records published to the topic.
 * <p>
 * In addition, when offsets are committed they are always committed for a given consumer group.
 * <p>
 * It is also possible for the consumer to manually specify the partitions it subscribes to, which disables this dynamic
 * partition balancing.
 * 
 * <h3>Usage Examples</h3>
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 * 
 * <h4>Simple Processing</h4>
 * This example demonstrates the simplest usage of Kafka's consumer api.
 * 
 * <pre>
 *     Properties props = new Properties();
 *     props.put(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.put(&quot;group.id&quot;, &quot;test&quot;);
 *     props.put(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     props.put(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.put(&quot;session.timeout.ms&quot;, &quot;30000&quot;);
 *     props.put(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.put(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;String, String&gt;(props);
 *     consumer.subscribe(&quot;foo&quot;, &quot;bar&quot;);
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(100);
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 * 
 * Setting <code>enable.auto.commit</code> means that offsets are committed automatically with a frequency controlled by
 * the config <code>auto.commit.interval.ms</code>.
 * <p>
 * The connection to the cluster is bootstrapped by specifying a list of one or more brokers to contact using the
 * configuration <code>bootstrap.servers</code>. This list is just used to discover the rest of the brokers in the
 * cluster and need not be an exhaustive list of servers in the cluster (though you may want to specify more than one in
 * case there are servers down when the client is connecting).
 * <p>
 * In this example the client is subscribing to the topics <i>foo</i> and <i>bar</i> as part of a group of consumers
 * called <i>test</i> as described above.
 * <p>
 * The broker will automatically detect failed processes in the <i>test</i> group by using a heartbeat mechanism. The
 * consumer will automatically ping the cluster periodically, which let's the cluster know that it is alive. As long as
 * the consumer is able to do this it is considered alive and retains the right to consume from the partitions assigned
 * to it. If it stops heartbeating for a period of time longer than <code>session.timeout.ms</code> then it will be
 * considered dead and it's partitions will be assigned to another process.
 * <p>
 * The deserializer settings specify how to turn bytes into objects. For example, by specifying string deserializers, we
 * are saying that our record's key and value will just be simple strings.
 * 
 * <h4>Controlling When Messages Are Considered Consumed</h4>
 * 
 * In this example we will consume a batch of records and batch them up in memory, when we have sufficient records
 * batched we will insert them into a database. If we allowed offsets to auto commit as in the previous example messages
 * would be considered consumed after they were given out by the consumer, and it would be possible that our process
 * could fail after we have read messages into our in-memory buffer but before they had been inserted into the database.
 * To avoid this we will manually commit the offsets only once the corresponding messages have been inserted into the
 * database. This gives us exact control of when a message is considered consumed. This raises the opposite possibility:
 * the process could fail in the interval after the insert into the database but before the commit (even though this
 * would likely just be a few milliseconds, it is a possibility). In this case the process that took over consumption
 * would consume from last committed offset and would repeat the insert of the last batch of data. Used in this way
 * Kafka provides what is often called "at-least once delivery" guarantees, as each message will likely be delivered one
 * time but in failure cases could be duplicated.
 * 
 * <pre>
 *     Properties props = new Properties();
 *     props.put(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.put(&quot;group.id&quot;, &quot;test&quot;);
 *     props.put(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     props.put(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.put(&quot;session.timeout.ms&quot;, &quot;30000&quot;);
 *     props.put(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.put(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;String, String&gt;(props);
 *     consumer.subscribe(&quot;foo&quot;, &quot;bar&quot;);
 *     int commitInterval = 200;
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;ConsumerRecord&lt;String, String&gt;&gt;();
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(100);
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             buffer.add(record);
 *             if (buffer.size() &gt;= commitInterval) {
 *                 insertIntoDb(buffer);
 *                 consumer.commit(CommitType.SYNC);
 *                 buffer.clear();
 *             }
 *         }
 *     }
 * </pre>
 * 
 * <h4>Subscribing To Specific Partitions</h4>
 * 
 * In the previous examples we subscribed to the topics we were interested in and let Kafka give our particular process
 * a fair share of the partitions for those topics. This provides a simple load balancing mechanism so multiple
 * instances of our program can divided up the work of processing records.
 * <p>
 * In this mode the consumer will just get the partitions it subscribes to and if the consumer instance fails no attempt
 * will be made to rebalance partitions to other instances.
 * <p>
 * There are several cases where this makes sense:
 * <ul>
 * <li>The first case is if the process is maintaining some kind of local state associated with that partition (like a
 * local on-disk key-value store) and hence it should only get records for the partition it is maintaining on disk.
 * <li>Another case is if the process itself is highly available and will be restarted if it fails (perhaps using a
 * cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework). In
 * this case there is no need for Kafka to detect the failure and reassign the partition, rather the consuming process
 * will be restarted on another machine.
 * </ul>
 * <p>
 * This mode is easy to specify, rather than subscribing to the topic, the consumer just subscribes to particular
 * partitions:
 * 
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.subscribe(partition0);
 *     consumer.subscribe(partition1);
 * </pre>
 * 
 * The group that the consumer specifies is still used for committing offsets, but now the set of partitions will only
 * be changed if the consumer specifies new partitions, and no attempt at failure detection will be made.
 * <p>
 * It isn't possible to mix both subscription to specific partitions (with no load balancing) and to topics (with load
 * balancing) using the same consumer instance.
 * 
 * <h4>Managing Your Own Offsets</h4>
 * 
 * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a store of it's own
 * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
 * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
 * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
 * stronger than the default "at-least once" semantics you get with Kafka's offset commit functionality.
 * <p>
 * Here are a couple of examples of this type of usage:
 * <ul>
 * <li>If the results of the consumption are being stored in a relational database, storing the offset in the database
 * as well can allow committing both the results and offset in a single transaction. Thus either the transaction will
 * succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset
 * won't be updated.
 * <li>If the results are being stored in a local store it may be possible to store the offset there as well. For
 * example a search index could be built by subscribing to a particular partition and storing both the offset and the
 * indexed data together. If this is done in a way that is atomic, it is often possible to have it be the case that even
 * if a crash occurs that causes unsync'd data to be lost, whatever is left has the corresponding offset stored as well.
 * This means that in this case the indexing process that comes back having lost recent updates just resumes indexing
 * from what it has ensuring that no updates are lost.
 * </ul>
 * 
 * Each record comes with it's own offset, so to manage your own offset you just need to do the following:
 * <ol>
 * <li>Configure <code>enable.auto.commit=false</code>
 * <li>Use the offset provided with each {@link ConsumerRecord} to save your position.
 * <li>On restart restore the position of the consumer using {@link #seek(TopicPartition, long)}.
 * </ol>
 * 
 * This type of usage is simplest when the partition assignment is also done manually (this would be likely in the
 * search index use case described above). If the partition assignment is done automatically special care will also be
 * needed to handle the case where partition assignments change. This can be handled using a special callback specified
 * using <code>rebalance.callback.class</code>, which specifies an implementation of the interface
 * {@link ConsumerRebalanceCallback}. When partitions are taken from a consumer the consumer will want to commit its
 * offset for those partitions by implementing
 * {@link ConsumerRebalanceCallback#onPartitionsRevoked(Consumer, Collection)}. When partitions are assigned to a
 * consumer, the consumer will want to look up the offset for those new partitions an correctly initialize the consumer
 * to that position by implementing {@link ConsumerRebalanceCallback#onPartitionsAssigned(Consumer, Collection)}.
 * <p>
 * Another common use for {@link ConsumerRebalanceCallback} is to flush any caches the application maintains for
 * partitions that are moved elsewhere.
 * 
 * <h4>Controlling The Consumer's Position</h4>
 * 
 * In most use cases the consumer will simply consume records from beginning to end, periodically committing it's
 * position (either automatically or manually). However Kafka allows the consumer to manually control it's position,
 * moving forward or backwards in a partition at will. This means a consumer can re-consume older records, or skip to
 * the most recent records without actually consuming the intermediate records.
 * <p>
 * There are several instances where manually controlling the consumer's position can be useful.
 * <p>
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not
 * attempt to catch up processing all records, but rather just skip to the most recent records.
 * <p>
 * Another use case is for a system that maintains local state as described in the previous section. In such a system
 * the consumer will want to initialize it's position on start-up to whatever is contained in the local store. Likewise
 * if the local state is destroyed (say because the disk is lost) the state may be recreated on a new machine by
 * reconsuming all the data and recreating the state (assuming that Kafka is retaining sufficient history).
 * 
 * Kafka allows specifying the position using {@link #seek(TopicPartition, long)} to specify the new position. Special
 * methods for seeking to the earliest and latest offset the server maintains are also available (
 * {@link #seekToBeginning(TopicPartition...)} and {@link #seekToEnd(TopicPartition...)} respectively).
 * 
 *
 * <h3><a name="multithreaded">Multi-threaded Processing</a></h3>
 * 
 * The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized. Un-synchronized access will result in {@link ConcurrentModificationException}.
 *
 * <p>
 * The only exception to this rule is {@link #wakeup()}, which can safely be used from an external thread to
 * interrupt an active operation. In this case, a {@link ConsumerWakeupException} will be thrown from the thread
 * blocking on the operation. This can be used to shutdown the consumer from another thread. The following
 * snippet shows the typical pattern:
 *
 * <pre>
 * public class KafkaConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaConsumer consumer;
 *
 *     public void run() {
 *         try {
 *             consumer.subscribe("topic");
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(10000);
 *                 // Handle new records
 *             }
 *         } catch (ConsumerWakeupException e) {
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
 *
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * We have intentionally avoided implementing a particular threading model for processing. This leaves several
 * options for implementing multi-threaded processing of records.
 *
 * 
 * <h4>1. One Consumer Per Thread</h4>
 * 
 * A simple option is to give each thread it's own consumer instance. Here are the pros and cons of this approach:
 * <ul>
 * <li><b>PRO</b>: It is the easiest to implement
 * <li><b>PRO</b>: It is often the fastest as no inter-thread co-ordination is needed
 * <li><b>PRO</b>: It makes in-order processing on a per-partition basis very easy to implement (each thread just
 * processes messages in the order it receives them).
 * <li><b>CON</b>: More consumers means more TCP connections to the cluster (one per thread). In general Kafka handles
 * connections very efficiently so this is generally a small cost.
 * <li><b>CON</b>: Multiple consumers means more requests being sent to the server and slightly less batching of data
 * which can cause some drop in I/O throughput.
 * <li><b>CON</b>: The number of total threads across all processes will be limited by the total number of partitions.
 * </ul>
 * 
 * <h4>2. Decouple Consumption and Processing</h4>
 * 
 * Another alternative is to have one or more consumer threads that do all data consumption and hands off
 * {@link ConsumerRecords} instances to a blocking queue consumed by a pool of processor threads that actually handle
 * the record processing.
 * 
 * This option likewise has pros and cons:
 * <ul>
 * <li><b>PRO</b>: This option allows independently scaling the number of consumers and processors. This makes it
 * possible to have a single consumer that feeds many processor threads, avoiding any limitation on partitions.
 * <li><b>CON</b>: Guaranteeing order across the processors requires particular care as the threads will execute
 * independently an earlier chunk of data may actually be processed after a later chunk of data just due to the luck of
 * thread execution timing. For processing that has no ordering requirements this is not a problem.
 * <li><b>CON</b>: Manually committing the position becomes harder as it requires that all threads co-ordinate to ensure
 * that processing is complete for that partition.
 * </ul>
 * 
 * There are many possible variations on this approach. For example each processor thread can have it's own queue, and
 * the consumer threads can hash into these queues using the TopicPartition to ensure in-order consumption and simplify
 * commit.
 * 
 */
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final long NO_CURRENT_THREAD = -1L;
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final Coordinator coordinator;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Fetcher<K, V> fetcher;

    private final Time time;
    private final ConsumerNetworkClient client;
    private final Metrics metrics;
    private final SubscriptionState subscriptions;
    private final Metadata metadata;
    private final long retryBackoffMs;
    private final boolean autoCommit;
    private final long autoCommitIntervalMs;
    private boolean closed = false;

    // currentThread holds the threadId of the current thread accessing KafkaConsumer
    // and is used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);

    // TODO: This timeout controls how long we should wait before retrying a request. We should be able
    //       to leverage the work of KAFKA-2120 to get this value from configuration.
    private long requestTimeoutMs = 5000L;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * 
     * @param configs The consumer configs
     */
    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null, null);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, a
     * {@link ConsumerRebalanceCallback} implementation, a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * 
     * @param configs The consumer configs
     * @param callback A callback interface that the user can implement to manage customized offsets on the start and
     *            end of every rebalance operation.
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs,
                         ConsumerRebalanceCallback callback,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
            callback,
            keyDeserializer,
            valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration. Valid
     * configuration strings are documented at {@link ConsumerConfig} A consumer is instantiated by providing a
     * {@link java.util.Properties} object as configuration. Valid configuration strings are documented at
     * {@link ConsumerConfig}
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration and a
     * {@link ConsumerRebalanceCallback} implementation, a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * 
     * @param properties The consumer configuration properties
     * @param callback A callback interface that the user can implement to manage customized offsets on the start and
     *            end of every rebalance operation.
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties,
                         ConsumerRebalanceCallback callback,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
             callback,
             keyDeserializer,
             valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    private KafkaConsumer(ConsumerConfig config,
                          ConsumerRebalanceCallback callback,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer) {
        try {
            log.debug("Starting the Kafka consumer");
            if (callback == null)
                callback = config.getConfiguredInstance(ConsumerConfig.CONSUMER_REBALANCE_CALLBACK_CLASS_CONFIG,
                        ConsumerRebalanceCallback.class);
            this.time = new SystemTime();
            this.autoCommit = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            this.autoCommitIntervalMs = config.getLong(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);

            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                            TimeUnit.MILLISECONDS);
            String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
            String jmxPrefix = "kafka.consumer";
            if (clientId.length() <= 0)
                clientId = "consumer-" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
            List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            reporters.add(new JmxReporter(jmxPrefix));
            this.metrics = new Metrics(metricConfig, reporters, time);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            this.metadata.update(Cluster.bootstrap(addresses), 0);

            String metricGrpPrefix = "consumer";
            Map<String, String> metricsTags = new LinkedHashMap<String, String>();
            metricsTags.put("client-id", clientId);
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, metricsTags),
                    this.metadata,
                    clientId,
                    100, // a fixed large enough value will suffice
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
            this.client = new ConsumerNetworkClient(netClient, metadata, time, retryBackoffMs);
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase());
            this.subscriptions = new SubscriptionState(offsetResetStrategy);
            this.coordinator = new Coordinator(this.client,
                    config.getString(ConsumerConfig.GROUP_ID_CONFIG),
                    config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                    config.getString(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    this.subscriptions,
                    metrics,
                    metricGrpPrefix,
                    metricsTags,
                    this.time,
                    requestTimeoutMs,
                    retryBackoffMs,
                    wrapRebalanceCallback(callback));
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
                this.keyDeserializer.configure(config.originals(), true);
            } else {
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
                this.valueDeserializer.configure(config.originals(), false);
            } else {
                this.valueDeserializer = valueDeserializer;
            }
            this.fetcher = new Fetcher<K, V>(this.client,
                    config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                    config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                    config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                    this.keyDeserializer,
                    this.valueDeserializer,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricGrpPrefix,
                    metricsTags,
                    this.time,
                    this.retryBackoffMs);

            config.logUnused();

            if (autoCommit)
                scheduleAutoCommitTask(autoCommitIntervalMs);

            log.debug("Kafka consumer created");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            close(true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    /**
     * The set of partitions currently assigned to this consumer. If subscription happened by directly subscribing to
     * partitions using {@link #subscribe(TopicPartition...)} then this will simply return the list of partitions that
     * were subscribed to. If subscription was done by specifying only the topic using {@link #subscribe(String...)}
     * then this will give the set of topics currently assigned to the consumer (which may be none if the assignment
     * hasn't happened yet, or the partitions are in the process of getting reassigned).
     */
    public Set<TopicPartition> subscriptions() {
        acquire();
        try {
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Incrementally subscribes to the given list of topics and uses the consumer's group management functionality
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger -
     * <ul>
     * <li>Number of partitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * 
     * @param topics A variable list of topics that the consumer wants to subscribe to
     */
    @Override
    public void subscribe(String... topics) {
        acquire();
        try {
            log.debug("Subscribed to topic(s): {}", Utils.join(topics, ", "));
            for (String topic : topics)
                this.subscriptions.subscribe(topic);
            metadata.addTopics(topics);
        } finally {
            release();
        }
    }

    /**
     * Incrementally subscribes to a specific topic partition and does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change.
     * <p>
     *
     * @param partitions Partitions to incrementally subscribe to
     */
    @Override
    public void subscribe(TopicPartition... partitions) {
        acquire();
        try {
            log.debug("Subscribed to partitions(s): {}", Utils.join(partitions, ", "));
            for (TopicPartition tp : partitions) {
                this.subscriptions.subscribe(tp);
                metadata.addTopics(tp.topic());
            }
        } finally {
            release();
        }
    }

    /**
     * Unsubscribe from the specific topics. This will trigger a rebalance operation and records for this topic will not
     * be returned from the next {@link #poll(long) poll()} onwards
     * 
     * @param topics Topics to unsubscribe from
     */
    public void unsubscribe(String... topics) {
        acquire();
        try {
            log.debug("Unsubscribed from topic(s): {}", Utils.join(topics, ", "));
            // throw an exception if the topic was never subscribed to
            for (String topic : topics)
                this.subscriptions.unsubscribe(topic);
        } finally {
            release();
        }
    }

    /**
     * Unsubscribe from the specific topic partitions. records for these partitions will not be returned from the next
     * {@link #poll(long) poll()} onwards
     * 
     * @param partitions Partitions to unsubscribe from
     */
    public void unsubscribe(TopicPartition... partitions) {
        acquire();
        try {
            log.debug("Unsubscribed from partitions(s): {}", Utils.join(partitions, ", "));
            // throw an exception if the partition was never subscribed to
            for (TopicPartition partition : partitions)
                this.subscriptions.unsubscribe(partition);
        } finally {
            release();
        }
    }

    /**
     * Fetches data for the topics or partitions specified using one of the subscribe APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * The offset used for fetching the data is governed by whether or not {@link #seek(TopicPartition, long)} is used.
     * If {@link #seek(TopicPartition, long)} is used, it will use the specified offsets on startup and on every
     * rebalance, to consume data from that offset sequentially on every poll. If not, it will use the last checkpointed
     * offset using {@link #commit(Map, CommitType) commit(offsets, sync)} for the subscribed list of partitions.
     * 
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
     *            immediately with any records available now. Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     * 
     * @throws NoOffsetForPartitionException If there is no stored offset for a subscribed partition and no automatic
     *             offset reset policy has been configured.
     */
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        acquire();
        try {
            if (timeout < 0)
                throw new IllegalArgumentException("Timeout must not be negative");

            // poll for new data until the timeout expires
            long remaining = timeout;
            while (remaining >= 0) {
                long start = time.milliseconds();
                Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollOnce(remaining);
                long end = time.milliseconds();

                if (!records.isEmpty()) {
                    // if data is available, then return it, but first send off the
                    // next round of fetches to enable pipelining while the user is
                    // handling the fetched records.
                    fetcher.initFetches(metadata.fetch());
                    client.poll(0);
                    return new ConsumerRecords<K, V>(records);
                }

                remaining -= end - start;

                // nothing was available, so we should backoff before retrying
                if (remaining > 0) {
                    Utils.sleep(min(remaining, retryBackoffMs));
                    remaining -= time.milliseconds() - end;
                }
            }

            return ConsumerRecords.empty();
        } finally {
            release();
        }
    }

    /**
     * Do one round of polling. In addition to checking for new data, this does any needed
     * heart-beating, auto-commits, and offset updates.
     * @param timeout The maximum time to block in the underlying poll
     * @return The fetched records (may be empty)
     */
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {
        // TODO: Sub-requests should take into account the poll timeout (KAFKA-1894)
        coordinator.ensureCoordinatorKnown();

        // ensure we have partitions assigned if we expect to
        if (subscriptions.partitionsAutoAssigned())
            coordinator.ensurePartitionAssignment();

        // fetch positions if we have partitions we're subscribed to that we
        // don't know the offset for
        if (!subscriptions.hasAllFetchPositions())
            updateFetchPositions(this.subscriptions.missingFetchPositions());

        // init any new fetches (won't resend pending fetches)
        Cluster cluster = this.metadata.fetch();
        fetcher.initFetches(cluster);
        client.poll(timeout);
        return fetcher.fetchedRecords();
    }

    private void scheduleAutoCommitTask(final long interval) {
        DelayedTask task = new DelayedTask() {
            public void run(long now) {
                commit(CommitType.ASYNC);
                client.schedule(this, now + interval);
            }
        };
        client.schedule(task, time.milliseconds() + interval);
    }

    /**
     * Commits the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * Asynchronous commits (i.e. {@link CommitType#ASYNC} will not block. Any errors encountered during an asynchronous
     * commit are silently discarded. If you need to determine the result of an asynchronous commit, you should use
     * {@link #commit(Map, CommitType, ConsumerCommitCallback)}. Synchronous commits (i.e. {@link CommitType#SYNC})
     * block until either the commit succeeds or an unrecoverable error is encountered (in which case it is thrown
     * to the caller).
     *
     * @param offsets The list of offsets per partition that should be committed to Kafka.
     * @param commitType Control whether the commit is blocking
     */
    @Override
    public void commit(final Map<TopicPartition, Long> offsets, CommitType commitType) {
        commit(offsets, commitType, null);
    }

    /**
     * Commits the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * Asynchronous commits (i.e. {@link CommitType#ASYNC} will not block. Any errors encountered during an asynchronous
     * commit are either passed to the callback (if provided) or silently discarded. Synchronous commits (i.e.
     * {@link CommitType#SYNC}) block until either the commit succeeds or an unrecoverable error is encountered. In
     * this case, the error is either passed to the callback (if provided) or thrown to the caller.
     *
     * @param offsets The list of offsets per partition that should be committed to Kafka.
     * @param commitType Control whether the commit is blocking
     * @param callback Callback to invoke when the commit completes
     */
    @Override
    public void commit(final Map<TopicPartition, Long> offsets, CommitType commitType, ConsumerCommitCallback callback) {
        acquire();
        try {
            log.debug("Committing offsets ({}): {} ", commitType.toString().toLowerCase(), offsets);
            coordinator.commitOffsets(offsets, commitType, callback);
        } finally {
            release();
        }
    }

    /**
     * Commits offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * Asynchronous commits (i.e. {@link CommitType#ASYNC} will not block. Any errors encountered during an asynchronous
     * commit are either passed to the callback (if provided) or silently discarded. Synchronous commits (i.e.
     * {@link CommitType#SYNC}) block until either the commit succeeds or an unrecoverable error is encountered. In
     * this case, the error is either passed to the callback (if provided) or thrown to the caller.
     *
     * @param commitType Whether or not the commit should block until it is acknowledged.
     * @param callback Callback to invoke when the commit completes
     */
    @Override
    public void commit(CommitType commitType, ConsumerCommitCallback callback) {
        acquire();
        try {
            // need defensive copy to ensure offsets are not removed before completion (e.g. in rebalance)
            Map<TopicPartition, Long> allConsumed = new HashMap<TopicPartition, Long>(this.subscriptions.allConsumed());
            commit(allConsumed, commitType, callback);
        } finally {
            release();
        }
    }

    /**
     * Commits offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * Asynchronous commits (i.e. {@link CommitType#ASYNC} will not block. Any errors encountered during an asynchronous
     * commit are silently discarded. If you need to determine the result of an asynchronous commit, you should use
     * {@link #commit(CommitType, ConsumerCommitCallback)}. Synchronous commits (i.e. {@link CommitType#SYNC})
     * block until either the commit succeeds or an unrecoverable error is encountered (in which case it is thrown
     * to the caller).
     *
     * @param commitType Whether or not the commit should block until it is acknowledged.
     */
    @Override
    public void commit(CommitType commitType) {
        commit(commitType, null);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        acquire();
        try {
            log.debug("Seeking to offset {} for partition {}", offset, partition);
            this.subscriptions.seek(partition, offset);
        } finally {
            release();
        }
    }

    /**
     * Seek to the first offset for each of the given partitions
     */
    public void seekToBeginning(TopicPartition... partitions) {
        acquire();
        try {
            Collection<TopicPartition> parts = partitions.length == 0 ? this.subscriptions.assignedPartitions()
                    : Arrays.asList(partitions);
            for (TopicPartition tp : parts) {
                log.debug("Seeking to beginning of partition {}", tp);
                subscriptions.needOffsetReset(tp, OffsetResetStrategy.EARLIEST);
            }
        } finally {
            release();
        }
    }

    /**
     * Seek to the last offset for each of the given partitions
     */
    public void seekToEnd(TopicPartition... partitions) {
        acquire();
        try {
            Collection<TopicPartition> parts = partitions.length == 0 ? this.subscriptions.assignedPartitions()
                    : Arrays.asList(partitions);
            for (TopicPartition tp : parts) {
                log.debug("Seeking to end of partition {}", tp);
                subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);
            }
        } finally {
            release();
        }
    }

    /**
     * Returns the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * 
     * @param partition The partition to get the position for
     * @return The offset
     * @throws NoOffsetForPartitionException If a position hasn't been set for a given partition, and no reset policy is
     *             available.
     */
    public long position(TopicPartition partition) {
        acquire();
        try {
            if (!this.subscriptions.assignedPartitions().contains(partition))
                throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
            Long offset = this.subscriptions.consumed(partition);
            if (offset == null) {
                updateFetchPositions(Collections.singleton(partition));
                return this.subscriptions.consumed(partition);
            } else {
                return offset;
            }
        } finally {
            release();
        }
    }

    /**
     * Fetches the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call may block to do a remote call if the partition in question isn't assigned to this consumer or if the
     * consumer hasn't yet initialized it's cache of committed offsets.
     * 
     * @param partition The partition to check
     * @return The last committed offset or null if no offset has been committed
     * @throws NoOffsetForPartitionException If no offset has ever been committed by any process for the given
     *             partition.
     */
    @Override
    public long committed(TopicPartition partition) {
        acquire();
        try {
            Long committed;
            if (subscriptions.assignedPartitions().contains(partition)) {
                committed = this.subscriptions.committed(partition);
                if (committed == null) {
                    coordinator.refreshCommittedOffsetsIfNeeded();
                    committed = this.subscriptions.committed(partition);
                }
            } else {
                Map<TopicPartition, Long> offsets = coordinator.fetchCommittedOffsets(Collections.singleton(partition));
                committed = offsets.get(partition);
            }

            if (committed == null)
                throw new NoOffsetForPartitionException("No offset has been committed for partition " + partition);

            return committed;
        } finally {
            release();
        }
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     * 
     * @param topic The topic to get partition metadata for
     * @return The list of partitions
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        acquire();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (parts == null) {
                metadata.add(topic);
                client.awaitMetadataUpdate();
                parts = metadata.fetch().partitionsForTopic(topic);
            }
            return parts;
        } finally {
            release();
        }
    }

    /**
     * Get metadata about partitions for all topics. This method will issue a remote call to the
     * server.
     *
     * @return The map of topics and its partitions
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        acquire();
        try {
            return fetcher.getAllTopics(requestTimeoutMs);
        } finally {
            release();
        }
    }

    @Override
    public void close() {
        acquire();
        try {
            if (closed) return;
            close(false);
        } finally {
            release();
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link ConsumerWakeupException}.
     */
    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    private void close(boolean swallowException) {
        log.trace("Closing the Kafka consumer.");
        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        this.closed = true;
        ClientUtils.closeQuietly(metrics, "consumer metrics", firstException);
        ClientUtils.closeQuietly(client, "consumer network client", firstException);
        ClientUtils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
        ClientUtils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);
        log.debug("The Kafka consumer has closed.");
        if (firstException.get() != null && !swallowException) {
            throw new KafkaException("Failed to close kafka consumer", firstException.get());
        }
    }

    private Coordinator.RebalanceCallback wrapRebalanceCallback(final ConsumerRebalanceCallback callback) {
        return new Coordinator.RebalanceCallback() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                callback.onPartitionsAssigned(KafkaConsumer.this, partitions);
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                callback.onPartitionsRevoked(KafkaConsumer.this, partitions);
            }
        };
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @param partitions The partitions that needs updating fetch positions
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     */
    private void updateFetchPositions(Set<TopicPartition> partitions) {
        // refresh commits for all assigned partitions
        coordinator.refreshCommittedOffsetsIfNeeded();

        // then do any offset lookups in case some positions are not known
        fetcher.updateFetchPositions(partitions);
    }

    /*
     * Check that the consumer hasn't been closed.
     */
    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
     * supported).
     * @throws IllegalStateException if the consumer has been closed
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        ensureNotClosed();
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }
}
