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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.ConnectionState;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.internals.Heartbeat;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ConsumerMetadataRequest;
import org.apache.kafka.common.requests.ConsumerMetadataResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * The consumer is thread safe but generally will be used only from within a single thread. The consumer client has no
 * threads of it's own, all work is done in the caller's thread when calls are made on the various methods exposed.
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
 *     props.put(&quot;metadata.broker.list&quot;, &quot;localhost:9092&quot;);
 *     props.put(&quot;group.id&quot;, &quot;test&quot;);
 *     props.put(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     props.put(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.put(&quot;session.timeout.ms&quot;, &quot;30000&quot;);
 *     props.put(&quot;key.serializer&quot;, &quot;org.apache.kafka.common.serializers.StringSerializer&quot;);
 *     props.put(&quot;value.serializer&quot;, &quot;org.apache.kafka.common.serializers.StringSerializer&quot;);
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
 * configuration <code>metadata.broker.list</code>. This list is just used to discover the rest of the brokers in the
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
 * The serializers settings specify how to turn the objects the user provides into bytes. By specifying the string
 * serializers we are saying that our record's key and value will just be simple strings.
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
 *     props.put(&quot;metadata.broker.list&quot;, &quot;localhost:9092&quot;);
 *     props.put(&quot;group.id&quot;, &quot;test&quot;);
 *     props.put(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     props.put(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.put(&quot;session.timeout.ms&quot;, &quot;30000&quot;);
 *     props.put(&quot;key.serializer&quot;, &quot;org.apache.kafka.common.serializers.StringSerializer&quot;);
 *     props.put(&quot;value.serializer&quot;, &quot;org.apache.kafka.common.serializers.StringSerializer&quot;);
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
 * <h3>Multithreaded Processing</h3>
 * 
 * The Kafka consumer is threadsafe but coarsely synchronized. All network I/O happens in the thread of the application
 * making the call. We have intentionally avoided implementing a particular threading model for processing.
 * <p>
 * This leaves several options for implementing multi-threaded processing of records.
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
    private static final long EARLIEST_OFFSET_TIMESTAMP = -2L;
    private static final long LATEST_OFFSET_TIMESTAMP = -1L;
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final Time time;
    private final ConsumerMetrics metrics;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final SubscriptionState subscriptions;
    private final Metadata metadata;
    private final Heartbeat heartbeat;
    private final NetworkClient client;
    private final int maxWaitMs;
    private final int minBytes;
    private final int fetchSize;
    private final boolean autoCommit;
    private final long autoCommitIntervalMs;
    private final String group;
    private final long sessionTimeoutMs;
    private final long retryBackoffMs;
    private final String partitionAssignmentStrategy;
    private final AutoOffsetResetStrategy offsetResetStrategy;
    private final ConsumerRebalanceCallback rebalanceCallback;
    private final List<PartitionRecords<K, V>> records;
    private final boolean checkCrcs;
    private long lastCommitAttemptMs;
    private String consumerId;
    private Node consumerCoordinator;
    private boolean closed = false;
    private int generation;

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
        this(new ConsumerConfig(addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
             callback,
             keyDeserializer,
             valueDeserializer);
    }

    private static Map<String, Object> addDeserializerToConfig(Map<String, Object> configs,
                                                               Deserializer<?> keyDeserializer,
                                                               Deserializer<?> valueDeserializer) {
        Map<String, Object> newConfigs = new HashMap<String, Object>();
        newConfigs.putAll(configs);
        if (keyDeserializer != null)
            newConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        if (keyDeserializer != null)
            newConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        return newConfigs;
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
        this(new ConsumerConfig(addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
             callback,
             keyDeserializer,
             valueDeserializer);
    }

    private static Properties addDeserializerToConfig(Properties properties,
                                                      Deserializer<?> keyDeserializer,
                                                      Deserializer<?> valueDeserializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keyDeserializer != null)
            newProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        if (keyDeserializer != null)
            newProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        return newProperties;
    }

    @SuppressWarnings("unchecked")
    private KafkaConsumer(ConsumerConfig config,
                          ConsumerRebalanceCallback callback,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer) {
        log.debug("Starting the Kafka consumer");
        if (keyDeserializer == null)
            this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                                                Deserializer.class);
        else
            this.keyDeserializer = keyDeserializer;
        if (valueDeserializer == null)
            this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                                                  Deserializer.class);
        else
            this.valueDeserializer = valueDeserializer;
        if (callback == null)
            this.rebalanceCallback = config.getConfiguredInstance(ConsumerConfig.CONSUMER_REBALANCE_CALLBACK_CLASS_CONFIG,
                                                                  ConsumerRebalanceCallback.class);
        else
            this.rebalanceCallback = callback;
        this.time = new SystemTime();
        this.maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        this.minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        this.fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.group = config.getString(ConsumerConfig.GROUP_ID_CONFIG);
        this.records = new LinkedList<PartitionRecords<K, V>>();
        this.sessionTimeoutMs = config.getLong(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, time.milliseconds());
        this.partitionAssignmentStrategy = config.getString(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
        this.offsetResetStrategy = AutoOffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
                                                                         .toUpperCase());
        this.checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG);
        this.autoCommit = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        this.autoCommitIntervalMs = config.getLong(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        this.lastCommitAttemptMs = time.milliseconds();

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
        Metrics metrics = new Metrics(metricConfig, reporters, time);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.metadata = new Metadata(retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG));
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), 0);

        String metricsGroup = "consumer";
        Map<String, String> metricsTags = new LinkedHashMap<String, String>();
        metricsTags.put("client-id", clientId);
        long reconnectBackoffMs = config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG);
        int sendBuffer = config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG);
        int receiveBuffer = config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG);
        this.client = new NetworkClient(new Selector(metrics, time, metricsGroup, metricsTags),
                                        this.metadata,
                                        clientId,
                                        100,
                                        reconnectBackoffMs,
                                        sendBuffer,
                                        receiveBuffer);
        this.subscriptions = new SubscriptionState();
        this.metrics = new ConsumerMetrics(metrics, metricsGroup, metricsTags);

        config.logUnused();

        this.consumerCoordinator = null;
        this.consumerId = "";
        this.generation = -1;
        log.debug("Kafka consumer created");
    }

    /**
     * The set of partitions currently assigned to this consumer. If subscription happened by directly subscribing to
     * partitions using {@link #subscribe(TopicPartition...)} then this will simply return the list of partitions that
     * were subscribed to. If subscription was done by specifying only the topic using {@link #subscribe(String...)}
     * then this will give the set of topics currently assigned to the consumer (which may be none if the assignment
     * hasn't happened yet, or the partitions are in the process of getting reassigned).
     */
    public synchronized Set<TopicPartition> subscriptions() {
        return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
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
    public synchronized void subscribe(String... topics) {
        ensureNotClosed();
        log.debug("Subscribed to topic(s): ", Utils.join(topics, ", "));
        for (String topic : topics)
            this.subscriptions.subscribe(topic);
        metadata.addTopics(topics);
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
    public synchronized void subscribe(TopicPartition... partitions) {
        ensureNotClosed();
        log.debug("Subscribed to partitions(s): ", Utils.join(partitions, ", "));
        for (TopicPartition tp : partitions) {
            this.subscriptions.subscribe(tp);
            metadata.addTopics(tp.topic());
        }
    }

    /**
     * Unsubscribe from the specific topics. This will trigger a rebalance operation and records for this topic will not
     * be returned from the next {@link #poll(long) poll()} onwards
     * 
     * @param topics Topics to unsubscribe from
     */
    public synchronized void unsubscribe(String... topics) {
        ensureNotClosed();
        log.debug("Unsubscribed from topic(s): ", Utils.join(topics, ", "));
        // throw an exception if the topic was never subscribed to
        for (String topic : topics)
            this.subscriptions.unsubscribe(topic);
    }

    /**
     * Unsubscribe from the specific topic partitions. records for these partitions will not be returned from the next
     * {@link #poll(long) poll()} onwards
     * 
     * @param partitions Partitions to unsubscribe from
     */
    public synchronized void unsubscribe(TopicPartition... partitions) {
        ensureNotClosed();
        log.debug("Unsubscribed from partitions(s): ", Utils.join(partitions, ", "));
        // throw an exception if the partition was never subscribed to
        for (TopicPartition partition : partitions)
            this.subscriptions.unsubscribe(partition);
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
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, waits
     *            indefinitely. Must not be negative
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     * 
     * @throws NoOffsetForPartitionException If there is no stored offset for a subscribed partition and no automatic
     *             offset reset policy has been configured.
     */
    @Override
    public synchronized ConsumerRecords<K, V> poll(long timeout) {
        ensureNotClosed();
        long now = time.milliseconds();

        if (subscriptions.partitionsAutoAssigned()) {
            // get partition assignment if needed
            if (subscriptions.needsPartitionAssignment()) {
                joinGroup(now);
            } else if (!heartbeat.isAlive(now)) {
                log.error("Failed heartbeat check.");
                coordinatorDead();
            } else if (heartbeat.shouldHeartbeat(now)) {
                initiateHeartbeat(now);
            }
        }

        // fetch positions if we have partitions we're subscribed to that we
        // don't know the offset for
        if (!subscriptions.hasAllFetchPositions())
            fetchMissingPositionsOrResetThem(this.subscriptions.missingFetchPositions(), now);

        // maybe autocommit position
        if (shouldAutoCommit(now))
            commit(CommitType.ASYNC);

        /*
         * initiate any needed fetches, then block for the timeout the user specified
         */
        Cluster cluster = this.metadata.fetch();
        reinstateFetches(cluster, now);
        client.poll(timeout, now);

        /*
         * initiate a fetch request for any nodes that we just got a response from without blocking
         */
        reinstateFetches(cluster, now);
        client.poll(0, now);

        return new ConsumerRecords<K, V>(consumeBufferedRecords());
    }

    /**
     * Commits the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * A non-blocking commit will attempt to commit offsets asychronously. No error will be thrown if the commit fails.
     * A blocking commit will wait for a response acknowledging the commit. In the event of an error it will retry until
     * the commit succeeds.
     * 
     * @param offsets The list of offsets per partition that should be committed to Kafka.
     * @param commitType Control whether the commit is blocking
     */
    @Override
    public synchronized void commit(final Map<TopicPartition, Long> offsets, CommitType commitType) {
        ensureNotClosed();
        log.debug("Committing offsets ({}): {} ", commitType.toString().toLowerCase(), offsets);
        long now = time.milliseconds();
        this.lastCommitAttemptMs = now;
        if (!offsets.isEmpty()) {
            Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<TopicPartition, OffsetCommitRequest.PartitionData>(offsets.size());
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
                offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(entry.getValue(), now, ""));
            OffsetCommitRequest req = new OffsetCommitRequest(this.group, this.generation, this.consumerId, offsetData);

            RequestCompletionHandler handler = new RequestCompletionHandler() {
                public void onComplete(ClientResponse resp) {
                    if (resp.wasDisconnected()) {
                        handleDisconnect(resp, time.milliseconds());
                    } else {
                        OffsetCommitResponse response = new OffsetCommitResponse(resp.responseBody());
                        for (Map.Entry<TopicPartition, Short> entry : response.responseData().entrySet()) {
                            TopicPartition tp = entry.getKey();
                            short errorCode = entry.getValue();
                            long offset = offsets.get(tp);
                            if (errorCode == Errors.NONE.code()) {
                                log.debug("Committed offset {} for partition {}", offset, tp);
                                subscriptions.committed(tp, offset);
                            } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                                    || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                                coordinatorDead();
                            } else {
                                log.error("Error committing partition {} at offset {}: {}",
                                          tp,
                                          offset,
                                          Errors.forCode(errorCode).exception().getMessage());
                            }
                        }
                    }
                    metrics.commitLatency.record(resp.requestLatencyMs());
                }
            };

            if (commitType == CommitType.ASYNC) {
                this.initiateCoordinatorRequest(ApiKeys.OFFSET_COMMIT, req.toStruct(), handler, now);
                return;
            } else {
                boolean done;
                do {
                    ClientResponse response = blockingCoordinatorRequest(ApiKeys.OFFSET_COMMIT,
                                                                         req.toStruct(),
                                                                         handler,
                                                                         now);

                    // check for errors
                    done = true;
                    OffsetCommitResponse commitResponse = new OffsetCommitResponse(response.responseBody());
                    for (short errorCode : commitResponse.responseData().values()) {
                        if (errorCode != Errors.NONE.code())
                            done = false;
                    }
                    if (!done) {
                        log.debug("Error in offset commit, backing off for {} ms before retrying again.",
                                  this.retryBackoffMs);
                        Utils.sleep(this.retryBackoffMs);
                    }
                } while (!done);
            }
        }
    }

    /**
     * Commits offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * 
     * @param commitType Whether or not the commit should block until it is acknowledged.
     */
    @Override
    public synchronized void commit(CommitType commitType) {
        ensureNotClosed();
        commit(this.subscriptions.allConsumed(), commitType);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     */
    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
        ensureNotClosed();
        log.debug("Seeking to offset {} for partition {}", offset, partition);
        this.subscriptions.seek(partition, offset);
    }

    /**
     * Seek to the first offset for each of the given partitions
     */
    public synchronized void seekToBeginning(TopicPartition... partitions) {
        ensureNotClosed();
        Collection<TopicPartition> parts = partitions.length == 0 ? this.subscriptions.assignedPartitions()
                : Arrays.asList(partitions);
        for (TopicPartition tp : parts) {
            // TODO: list offset call could be optimized by grouping by node
            seek(tp, listOffset(tp, EARLIEST_OFFSET_TIMESTAMP));
        }
    }

    /**
     * Seek to the last offset for each of the given partitions
     */
    public synchronized void seekToEnd(TopicPartition... partitions) {
        ensureNotClosed();
        Collection<TopicPartition> parts = partitions.length == 0 ? this.subscriptions.assignedPartitions()
                : Arrays.asList(partitions);
        for (TopicPartition tp : parts) {
            // TODO: list offset call could be optimized by grouping by node
            seek(tp, listOffset(tp, LATEST_OFFSET_TIMESTAMP));
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
    public synchronized long position(TopicPartition partition) {
        ensureNotClosed();
        if (!this.subscriptions.assignedPartitions().contains(partition))
            throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
        Long offset = this.subscriptions.consumed(partition);
        if (offset == null) {
            fetchMissingPositionsOrResetThem(Collections.singleton(partition), time.milliseconds());
            return this.subscriptions.consumed(partition);
        } else {
            return offset;
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
    public synchronized long committed(TopicPartition partition) {
        ensureNotClosed();
        Set<TopicPartition> partitionsToFetch;
        if (subscriptions.assignedPartitions().contains(partition)) {
            Long committed = this.subscriptions.committed(partition);
            if (committed != null)
                return committed;
            partitionsToFetch = subscriptions.assignedPartitions();
        } else {
            partitionsToFetch = Collections.singleton(partition);
        }
        this.refreshCommittedOffsets(time.milliseconds(), partitionsToFetch);
        Long committed = this.subscriptions.committed(partition);
        if (committed == null)
            throw new NoOffsetForPartitionException("No offset has been committed for partition " + partition);
        return committed;
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics.metrics());
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
        Cluster cluster = this.metadata.fetch();
        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        if (parts == null) {
            metadata.add(topic);
            awaitMetadataUpdate();
            parts = metadata.fetch().partitionsForTopic(topic);
        }
        return parts;
    }

    @Override
    public synchronized void close() {
        log.trace("Closing the Kafka consumer.");
        this.closed = true;
        this.metrics.metrics.close();
        this.client.close();
        log.debug("The Kafka consumer has closed.");
    }

    private boolean shouldAutoCommit(long now) {
        return this.autoCommit && this.lastCommitAttemptMs <= now - this.autoCommitIntervalMs;
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

    /*
     * Send a join group request to the controller
     */
    private void joinGroup(long now) {
        log.debug("Joining group {}", group);

        // execute the user's callback
        try {
            // TODO: Hmmm, is passing the full Consumer like this actually safe?
            // Need to think about reentrancy...
            this.rebalanceCallback.onPartitionsRevoked(this, this.subscriptions.assignedPartitions());
        } catch (Exception e) {
            log.error("User provided callback " + this.rebalanceCallback.getClass().getName()
                    + " failed on partition revocation: ", e);
        }

        // join the group
        JoinGroupRequest jgr = new JoinGroupRequest(group,
                                                    (int) this.sessionTimeoutMs,
                                                    new ArrayList<String>(this.subscriptions.subscribedTopics()),
                                                    this.consumerId,
                                                    this.partitionAssignmentStrategy);
        ClientResponse resp = this.blockingCoordinatorRequest(ApiKeys.JOIN_GROUP, jgr.toStruct(), null, now);
        // process the response
        JoinGroupResponse response = new JoinGroupResponse(resp.responseBody());
        log.debug("Joined group: {}", response);
        Errors.forCode(response.errorCode()).maybeThrow();
        this.consumerId = response.consumerId();
        this.subscriptions.changePartitionAssignment(response.assignedPartitions());
        this.heartbeat.receivedResponse(now);

        // execute the callback
        try {
            // TODO: Hmmm, is passing the full Consumer like this actually safe?
            this.rebalanceCallback.onPartitionsAssigned(this, this.subscriptions.assignedPartitions());
        } catch (Exception e) {
            log.error("User provided callback " + this.rebalanceCallback.getClass().getName()
                    + " failed on partition assignment: ", e);
        }

        // record re-assignment time
        this.metrics.partitionReassignments.record(time.milliseconds() - now);
    }

    /*
     * Empty the record buffer and update the consumed position.
     */
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> consumeBufferedRecords() {
        if (this.subscriptions.needsPartitionAssignment()) {
            return Collections.emptyMap();
        } else {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
            for (PartitionRecords<K, V> part : this.records) {
                Long consumed = subscriptions.consumed(part.partition);
                if (this.subscriptions.assignedPartitions().contains(part.partition)
                        && (consumed == null || part.fetchOffset == consumed)) {
                    List<ConsumerRecord<K, V>> partRecs = drained.get(part.partition);
                    if (partRecs == null) {
                        partRecs = part.records;
                        drained.put(part.partition, partRecs);
                    } else {
                        partRecs.addAll(part.records);
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

    /*
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't have one
     */
    private void reinstateFetches(Cluster cluster, long now) {
        for (ClientRequest request : createFetchRequests(cluster)) {
            Node node = cluster.nodeById(request.request().destination());
            if (client.ready(node, now)) {
                log.trace("Initiating fetch to node {}: {}", node.id(), request);
                client.send(request);
            }
        }
    }

    /*
     * Create fetch requests for all nodes for which we have assigned partitions that have no existing requests in
     * flight
     */
    private List<ClientRequest> createFetchRequests(Cluster cluster) {
        Map<Integer, Map<TopicPartition, PartitionData>> fetchable = new HashMap<Integer, Map<TopicPartition, PartitionData>>();
        for (TopicPartition partition : subscriptions.assignedPartitions()) {
            Node node = cluster.leaderFor(partition);
            // if there is a leader and no in-flight requests, issue a new fetch
            if (node != null && this.client.inFlightRequestCount(node.id()) == 0) {
                Map<TopicPartition, PartitionData> fetch = fetchable.get(node);
                if (fetch == null) {
                    fetch = new HashMap<TopicPartition, PartitionData>();
                    fetchable.put(node.id(), fetch);
                }
                long offset = this.subscriptions.fetched(partition);
                fetch.put(partition, new FetchRequest.PartitionData(offset, this.fetchSize));
            }
        }
        List<ClientRequest> requests = new ArrayList<ClientRequest>(fetchable.size());
        for (Map.Entry<Integer, Map<TopicPartition, PartitionData>> entry : fetchable.entrySet()) {
            int nodeId = entry.getKey();
            final FetchRequest fetch = new FetchRequest(this.maxWaitMs, minBytes, entry.getValue());
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

    private void handleFetchResponse(ClientResponse resp, FetchRequest request) {
        if (resp.wasDisconnected()) {
            handleDisconnect(resp, time.milliseconds());
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
                    ByteBuffer buffer = partition.recordSet;
                    buffer.position(buffer.limit()); // TODO: arguably we should not have to muck with the position here
                    MemoryRecords records = MemoryRecords.readableRecords(buffer);
                    long fetchOffset = request.fetchData().get(tp).offset;
                    int bytes = 0;
                    List<ConsumerRecord<K, V>> parsed = new ArrayList<ConsumerRecord<K, V>>();
                    for (LogEntry logEntry : records) {
                        parsed.add(parseRecord(tp, logEntry));
                        bytes += logEntry.size();
                    }
                    if (parsed.size() > 0) {
                        ConsumerRecord<K, V> record = parsed.get(parsed.size() - 1);
                        this.subscriptions.fetched(tp, record.offset() + 1);
                        this.metrics.lag.record(partition.highWatermark - record.offset());
                        this.records.add(new PartitionRecords<K, V>(fetchOffset, tp, parsed));
                    }
                    this.metrics.recordTopicFetchMetrics(tp.topic(), bytes, parsed.size());
                    totalBytes += bytes;
                    totalCount += parsed.size();
                } else if (partition.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                        || partition.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()
                        || partition.errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
                    this.metadata.requestUpdate();
                } else if (partition.errorCode == Errors.OFFSET_OUT_OF_RANGE.code()) {
                    // TODO: this could be optimized by grouping all out-of-range partitions
                    resetOffset(tp, time.milliseconds());
                }
            }
            this.metrics.bytesFetched.record(totalBytes);
            this.metrics.recordsFetched.record(totalCount);
        }
        this.metrics.fetchLatency.record(resp.requestLatencyMs());
    }

    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        if (this.checkCrcs)
            logEntry.record().ensureValid();
        long offset = logEntry.offset();
        ByteBuffer keyBytes = logEntry.record().key();
        K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), Utils.toArray(keyBytes));
        ByteBuffer valueBytes = logEntry.record().value();
        V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(),
                                                                                 Utils.toArray(valueBytes));
        return new ConsumerRecord<K, V>(partition.topic(), partition.partition(), offset, key, value);
    }

    /*
     * Begin sending a heartbeat to the controller but don't wait for the response
     */
    private void initiateHeartbeat(long now) {
        ensureCoordinatorReady();
        log.debug("Sending heartbeat to co-ordinator.");
        HeartbeatRequest req = new HeartbeatRequest(this.group, this.generation, this.consumerId);
        RequestSend send = new RequestSend(this.consumerCoordinator.id(),
                                           this.client.nextRequestHeader(ApiKeys.HEARTBEAT),
                                           req.toStruct());

        RequestCompletionHandler handler = new RequestCompletionHandler() {
            public void onComplete(ClientResponse resp) {
                if (resp.wasDisconnected()) {
                    coordinatorDead();
                } else {
                    HeartbeatResponse response = new HeartbeatResponse(resp.responseBody());
                    if (response.errorCode() == Errors.NONE.code()) {
                        log.debug("Received successful heartbeat response.");
                        heartbeat.receivedResponse(time.milliseconds());
                    } else if (response.errorCode() == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                            || response.errorCode() == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        coordinatorDead();
                    } else {
                        throw new KafkaException("Unexpected error in hearbeat response: "
                                + Errors.forCode(response.errorCode()).exception().getMessage());
                    }
                }
                metrics.heartbeatLatency.record(resp.requestLatencyMs());
            }
        };
        this.client.send(new ClientRequest(now, true, send, handler));
        this.heartbeat.sentHeartbeat(now);
    }

    private void coordinatorDead() {
        log.info("Marking the co-ordinator dead.");
        heartbeat.markDead();
        if (subscriptions.partitionsAutoAssigned())
            subscriptions.clearAssignment();
        this.consumerCoordinator = null;
    }

    /*
     * Initiate a request to the co-ordinator but don't wait for a response.
     */
    private void initiateCoordinatorRequest(ApiKeys api, Struct request, RequestCompletionHandler handler, long now) {
        log.debug("Issuing co-ordinator request: {}: {}", api, request);
        ensureCoordinatorReady();
        RequestHeader header = this.client.nextRequestHeader(api);
        RequestSend send = new RequestSend(this.consumerCoordinator.id(), header, request);
        ClientRequest clientRequest = new ClientRequest(now, true, send, handler);
        this.client.send(clientRequest);
    }

    /*
     * Repeatedly attempt to send a request to the co-ordinator until a response is received (retry if we are
     * disconnected). Note that this means any requests sent this way must be idempotent.
     * 
     * @return The response
     */
    private ClientResponse blockingCoordinatorRequest(ApiKeys api,
                                                      Struct request,
                                                      RequestCompletionHandler handler,
                                                      long now) {
        do {
            initiateCoordinatorRequest(api, request, handler, now);
            List<ClientResponse> responses = this.client.completeAll(consumerCoordinator.id(), now);
            if (responses.size() == 0) {
                throw new IllegalStateException("This should not happen.");
            } else {
                ClientResponse response = responses.get(responses.size() - 1);
                if (response.wasDisconnected()) {
                    handleDisconnect(response, time.milliseconds());
                    Utils.sleep(this.retryBackoffMs);
                } else {
                    return response;
                }
            }
        } while (true);
    }

    /*
     * update the current consumer co-ordinator if needed and ensure we have a ready connection to it
     */
    private void ensureCoordinatorReady() {
        while (true) {
            if (this.consumerCoordinator == null)
                discoverCoordinator();

            while (true) {
                boolean ready = this.client.ready(this.consumerCoordinator, time.milliseconds());
                if (ready) {
                    return;
                } else {
                    log.debug("No connection to co-ordinator, attempting to connect.");
                    this.client.poll(this.retryBackoffMs, time.milliseconds());
                    ConnectionState state = this.client.connectionState(this.consumerCoordinator.id());
                    if (ConnectionState.DISCONNECTED.equals(state)) {
                        log.debug("Co-ordinator connection failed. Attempting to re-discover.");
                        coordinatorDead();
                        break;
                    }
                }
            }
        }
    }

    private void discoverCoordinator() {
        while (this.consumerCoordinator == null) {
            log.debug("No consumer co-ordinator known, attempting to discover one.");
            Node coordinator = fetchConsumerCoordinator();

            if (coordinator == null) {
                log.debug("No co-ordinator found, backing off.");
                Utils.sleep(this.retryBackoffMs);
            } else {
                log.debug("Found consumer co-ordinator: " + coordinator);
                this.consumerCoordinator = coordinator;
            }
        }
    }

    private Node fetchConsumerCoordinator() {
        // find a node to ask about the co-ordinator
        Node node = this.client.leastLoadedNode(time.milliseconds());
        while (node == null || !this.client.ready(node, time.milliseconds())) {
            long now = time.milliseconds();
            this.client.poll(this.retryBackoffMs, now);
            node = this.client.leastLoadedNode(now);
        }

        // send the metadata request and process all responses
        long now = time.milliseconds();
        this.client.send(createConsumerMetadataRequest(now));
        List<ClientResponse> responses = this.client.completeAll(node.id(), now);
        if (responses.isEmpty()) {
            throw new IllegalStateException("This should not happen.");
        } else {
            ClientResponse resp = responses.get(responses.size() - 1);
            if (!resp.wasDisconnected()) {
                ConsumerMetadataResponse response = new ConsumerMetadataResponse(resp.responseBody());
                if (response.errorCode() == Errors.NONE.code())
                    return new Node(Integer.MAX_VALUE - response.node().id(), response.node().host(), response.node().port());
            }
        }
        return null;
    }

    /**
     * Update our cache of committed positions and then set the fetch position to the committed position (if there is
     * one) or reset it using the offset reset policy the user has configured.
     * 
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     */
    private void fetchMissingPositionsOrResetThem(Set<TopicPartition> partitions, long now) {
        // update the set of committed offsets
        refreshCommittedOffsets(now, partitions);

        // reset the fetch position to the committed poisition
        for (TopicPartition tp : partitions) {
            if (subscriptions.fetched(tp) == null) {
                if (subscriptions.committed(tp) == null) {
                    resetOffset(tp, now);
                } else {
                    log.debug("Resetting offset for partition {} to committed offset");
                    subscriptions.seek(tp, subscriptions.committed(tp));
                }
            }
        }
    }

    /*
     * Fetch the given set of partitions and update the cache of committed offsets using the result
     */
    private void refreshCommittedOffsets(long now, Set<TopicPartition> partitions) {
        log.debug("Fetching committed offsets for partitions: " + Utils.join(partitions, ", "));
        OffsetFetchRequest request = new OffsetFetchRequest(this.group, new ArrayList<TopicPartition>(partitions));
        ClientResponse resp = this.blockingCoordinatorRequest(ApiKeys.OFFSET_FETCH, request.toStruct(), null, now);
        OffsetFetchResponse response = new OffsetFetchResponse(resp.responseBody());
        for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetFetchResponse.PartitionData data = entry.getValue();
            if (data.hasError()) {
                log.debug("Error fetching offset for topic-partition {}: {}", tp, Errors.forCode(data.errorCode)
                                                                                        .exception()
                                                                                        .getMessage());
            } else if (data.offset >= 0) {
                // update the position with the offset (-1 seems to indicate no
                // such offset known)
                this.subscriptions.committed(tp, data.offset);
            } else {
                log.debug("No committed offset for partition " + tp);
            }
        }
    }

    /*
     * Fetch a single offset before the given timestamp for the partition.
     */
    private long listOffset(TopicPartition tp, long ts) {
        log.debug("Fetching offsets for partition {}.", tp);
        Map<TopicPartition, ListOffsetRequest.PartitionData> partitions = new HashMap<TopicPartition, ListOffsetRequest.PartitionData>(1);
        partitions.put(tp, new ListOffsetRequest.PartitionData(ts, 1));
        while (true) {
            long now = time.milliseconds();
            PartitionInfo info = metadata.fetch().partition(tp);
            if (info == null) {
                metadata.add(tp.topic());
                awaitMetadataUpdate();
            } else if (info.leader() == null) {
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
                    short errorCode = lor.responseData().get(tp).errorCode;
                    if (errorCode == Errors.NONE.code()) {
                        List<Long> offsets = lor.responseData().get(tp).offsets;
                        if (offsets.size() != 1)
                            throw new IllegalStateException("This should not happen.");
                        return offsets.get(0);
                    } else if (errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                            || errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
                        log.warn("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                                 tp);
                        awaitMetadataUpdate();
                        continue;
                    } else {
                        Errors.forCode(errorCode).maybeThrow();
                    }
                }
            } else {
                client.poll(this.retryBackoffMs, now);
            }
        }
    }

    /*
     * Create a consumer metadata request for the given group
     */
    private ClientRequest createConsumerMetadataRequest(long now) {
        ConsumerMetadataRequest request = new ConsumerMetadataRequest(this.group);
        Node destination = this.client.leastLoadedNode(now);
        if (destination == null) // all nodes are blacked out
            return null;
        RequestSend send = new RequestSend(destination.id(),
                                           this.client.nextRequestHeader(ApiKeys.CONSUMER_METADATA),
                                           request.toStruct());
        ClientRequest consumerMetadataRequest = new ClientRequest(now, true, send, null);
        return consumerMetadataRequest;
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy
     * 
     * @throws NoOffsetForPartitionException If no offset reset strategy is defined
     */
    private void resetOffset(TopicPartition partition, long now) {
        long timestamp;
        if (this.offsetResetStrategy == AutoOffsetResetStrategy.EARLIEST)
            timestamp = EARLIEST_OFFSET_TIMESTAMP;
        else if (this.offsetResetStrategy == AutoOffsetResetStrategy.LATEST)
            timestamp = LATEST_OFFSET_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException("No offset is set and no reset policy is defined");

        log.debug("Resetting offset for partition {} to {} offset.", partition, this.offsetResetStrategy.name()
                                                                                                        .toLowerCase());
        this.subscriptions.seek(partition, listOffset(partition, timestamp));
    }

    private void handleDisconnect(ClientResponse response, long now) {
        int correlation = response.request().request().header().correlationId();
        log.debug("Cancelled request {} with correlation id {} due to node {} being disconnected",
                  response.request(),
                  correlation,
                  response.request().request().destination());
        if (this.consumerCoordinator != null
                && response.request().request().destination() == this.consumerCoordinator.id())
            coordinatorDead();
    }

    /*
     * Check that the consumer hasn't been closed.
     */
    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
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
        LATEST, EARLIEST, NONE;
    }

    private class ConsumerMetrics {
        public final Metrics metrics;
        public final Sensor bytesFetched;
        public final Sensor recordsFetched;
        public final Sensor fetchLatency;
        public final Sensor commitLatency;
        public final Sensor partitionReassignments;
        public final Sensor heartbeatLatency;
        public final Sensor lag;

        public ConsumerMetrics(Metrics metrics, String metricsGroup, Map<String, String> tags) {
            this.metrics = metrics;

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(new MetricName("fetch-size-avg",
                                                 metricsGroup,
                                                 "The average number of bytes fetched per request",
                                                 tags), new Avg());
            this.bytesFetched.add(new MetricName("fetch-size-max",
                                                 metricsGroup,
                                                 "The maximum number of bytes fetched per request",
                                                 tags), new Max());
            this.bytesFetched.add(new MetricName("bytes-consumed-rate",
                                                 metricsGroup,
                                                 "The average number of bytes consumed per second",
                                                 tags), new Rate());

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(new MetricName("records-per-request-avg",
                                                   metricsGroup,
                                                   "The average number of records in each request",
                                                   tags), new Avg());
            this.recordsFetched.add(new MetricName("records-consumed-rate",
                                                   metricsGroup,
                                                   "The average number of records consumed per second",
                                                   tags), new Rate());

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(new MetricName("fetch-latency-avg",
                                                 metricsGroup,
                                                 "The average time taken for a fetch request.",
                                                 tags), new Avg());
            this.fetchLatency.add(new MetricName("fetch-latency-max",
                                                 metricsGroup,
                                                 "The max time taken for any fetch request.",
                                                 tags), new Max());
            this.fetchLatency.add(new MetricName("fetch-rate",
                                                 metricsGroup,
                                                 "The number of fetch requests per second.",
                                                 tags), new Rate(new Count()));

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(new MetricName("commit-latency-avg",
                                                  metricsGroup,
                                                  "The average time taken for a commit request",
                                                  tags), new Avg());
            this.commitLatency.add(new MetricName("commit-latency-max",
                                                  metricsGroup,
                                                  "The max time taken for a commit request",
                                                  tags), new Max());
            this.commitLatency.add(new MetricName("commit-rate",
                                                  metricsGroup,
                                                  "The number of commit calls per second",
                                                  tags), new Rate(new Count()));

            this.partitionReassignments = metrics.sensor("reassignment-latency");
            this.partitionReassignments.add(new MetricName("reassignment-time-avg",
                                                           metricsGroup,
                                                           "The average time taken for a partition reassignment",
                                                           tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-time-max",
                                                           metricsGroup,
                                                           "The max time taken for a partition reassignment",
                                                           tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-rate",
                                                           metricsGroup,
                                                           "The number of partition reassignments per second",
                                                           tags), new Rate(new Count()));

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(new MetricName("heartbeat-response-time-max",
                                                     metricsGroup,
                                                     "The max time taken to receive a response to a hearbeat request",
                                                     tags), new Max());
            this.heartbeatLatency.add(new MetricName("heartbeat-rate",
                                                     metricsGroup,
                                                     "The average number of heartbeats per second",
                                                     tags), new Rate(new Count()));

            this.lag = metrics.sensor("lag");
            this.lag.add(new MetricName("lag-max",
                                        metricsGroup,
                                        "The maximum lag for any partition in this window",
                                        tags), new Max());

            Measurable numParts = 
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(new MetricName("assigned-partitions",
                                             metricsGroup,
                                             "The number of partitions currently assigned to this consumer",
                                             tags),
                              numParts);
                              
            
            Measurable lastHeartbeat =                               
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(new MetricName("last-heartbeat-seconds-ago",
                                             metricsGroup,
                                             "The number of seconds since the last controller heartbeat",
                                             tags), 
                                             
                              lastHeartbeat);
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
            recordsFetched.record(bytes);
        }
    }
}
