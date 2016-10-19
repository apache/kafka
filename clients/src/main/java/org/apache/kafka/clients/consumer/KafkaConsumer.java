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
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.PollCondition;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * A client that consumes records from a Kafka cluster.
 * <p>
 * This client transparently handles the failure of Kafka brokers, and transparently adapts as topic partitions
 * it fetches migrate within the cluster. This client also interacts with the broker to allow groups of
 * consumers to load balance consumption using <a href="#consumergroups">consumer groups</a>.
 * <p>
 * The consumer maintains TCP connections to the necessary brokers to fetch data.
 * Failure to close the consumer after use will leak these connections.
 * The consumer is not thread-safe. See <a href="#multithreaded">Multi-threaded Processing</a> for more details.
 *
 * <h3>Offsets and Consumer Position</h3>
 * Kafka maintains a numerical offset for each record in a partition. This offset acts as a unique identifier of
 * a record within that partition, and also denotes the position of the consumer in the partition. For example, a consumer
 * which is at position 5 has consumed records with offsets 0 through 4 and will next receive the record with offset 5. There
 * are actually two notions of position relevant to the user of the consumer:
 * <p>
 * The {@link #position(TopicPartition) position} of the consumer gives the offset of the next record that will be given
 * out. It will be one larger than the highest offset the consumer has seen in that partition. It automatically advances
 * every time the consumer receives messages in a call to {@link #poll(long)}.
 * <p>
 * The {@link #commitSync() committed position} is the last offset that has been stored securely. Should the
 * process fail and restart, this is the offset that the consumer will recover to. The consumer can either automatically commit
 * offsets periodically; or it can choose to control this committed position manually by calling one of the commit APIs
 * (e.g. {@link #commitSync() commitSync} and {@link #commitAsync(OffsetCommitCallback) commitAsync}).
 * <p>
 * This distinction gives the consumer control over when a record is considered consumed. It is discussed in further
 * detail below.
 *
 * <h3><a name="consumergroups">Consumer Groups and Topic Subscriptions</a></h3>
 *
 * Kafka uses the concept of <i>consumer groups</i> to allow a pool of processes to divide the work of consuming and
 * processing records. These processes can either be running on the same machine or they can be
 * distributed over many machines to provide scalability and fault tolerance for processing. All consumer instances
 * sharing the same <code>group.id</code> will be part of the same consumer group.
 * <p>
 * Each consumer in a group can dynamically set the list of topics it wants to subscribe to through one of the
 * {@link #subscribe(Collection, ConsumerRebalanceListener) subscribe} APIs. Kafka will deliver each message in the
 * subscribed topics to one process in each consumer group. This is achieved by balancing the partitions between all
 * members in the consumer group so that each partition is assigned to exactly one consumer in the group. So if there
 * is a topic with four partitions, and a consumer group with two processes, each process would consume from two partitions.
 * <p>
 * Membership in a consumer group is maintained dynamically: if a process fails, the partitions assigned to it will
 * be reassigned to other consumers in the same group. Similarly, if a new consumer joins the group, partitions will be moved
 * from existing consumers to the new one. This is known as <i>rebalancing</i> the group and is discussed in more
 * detail <a href="#failuredetection">below</a>. Group rebalancing is also used when new partitions are added
 * to one of the subscribed topics or when a new topic matching a {@link #subscribe(Pattern, ConsumerRebalanceListener) subscribed regex}
 * is created. The group will automatically detect the new partitions through periodic metadata refreshes and
 * assign them to members of the group.
 * <p>
 * Conceptually you can think of a consumer group as being a single logical subscriber that happens to be made up of
 * multiple processes. As a multi-subscriber system, Kafka naturally supports having any number of consumer groups for a
 * given topic without duplicating data (additional consumers are actually quite cheap).
 * <p>
 * This is a slight generalization of the functionality that is common in messaging systems. To get semantics similar to
 * a queue in a traditional messaging system all processes would be part of a single consumer group and hence record
 * delivery would be balanced over the group like with a queue. Unlike a traditional messaging system, though, you can
 * have multiple such groups. To get semantics similar to pub-sub in a traditional messaging system each process would
 * have its own consumer group, so each process would subscribe to all the records published to the topic.
 * <p>
 * In addition, when group reassignment happens automatically, consumers can be notified through a {@link ConsumerRebalanceListener},
 * which allows them to finish necessary application-level logic such as state cleanup, manual offset
 * commits, etc. See <a href="#rebalancecallback">Storing Offsets Outside Kafka</a> for more details.
 * <p>
 * It is also possible for the consumer to <a href="#manualassignment">manually assign</a> specific partitions
 * (similar to the older "simple" consumer) using {@link #assign(Collection)}. In this case, dynamic partition
 * assignment and consumer group coordination will be disabled.
 *
 * <h3><a name="failuredetection">Detecting Consumer Failures</a></h3>
 *
 * After subscribing to a set of topics, the consumer will automatically join the group when {@link #poll(long)} is
 * invoked. The poll API is designed to ensure consumer liveness. As long as you continue to call poll, the consumer
 * will stay in the group and continue to receive messages from the partitions it was assigned. Underneath the covers,
 * the consumer sends periodic heartbeats to the server. If the consumer crashes or is unable to send heartbeats for
 * a duration of <code>session.timeout.ms</code>, then the consumer will be considered dead and its partitions will
 * be reassigned.
 * <p>
 * It is also possible that the consumer could encounter a "livelock" situation where it is continuing
 * to send heartbeats, but no progress is being made. To prevent the consumer from holding onto its partitions
 * indefinitely in this case, we provide a liveness detection mechanism using the <code>max.poll.interval.ms</code>
 * setting. Basically if you don't call poll at least as frequently as the configured max interval,
 * then the client will proactively leave the group so that another consumer can take over its partitions. When this happens,
 * you may see an offset commit failure (as indicated by a {@link CommitFailedException} thrown from a call to {@link #commitSync()}).
 * This is a safety mechanism which guarantees that only active members of the group are able to commit offsets.
 * So to stay in the group, you must continue to call poll.
 * <p>
 * The consumer provides two configuration settings to control the behavior of the poll loop:
 * <ol>
 *     <li><code>max.poll.interval.ms</code>: By increasing the interval between expected polls, you can give
 *     the consumer more time to handle a batch of records returned from {@link #poll(long)}. The drawback
 *     is that increasing this value may delay a group rebalance since the consumer will only join the rebalance
 *     inside the call to poll. You can use this setting to bound the time to finish a rebalance, but
 *     you risk slower progress if the consumer cannot actually call {@link #poll(long) poll} often enough.</li>
 *     <li><code>max.poll.records</code>: Use this setting to limit the total records returned from a single
 *     call to poll. This can make it easier to predict the maximum that must be handled within each poll
 *     interval. By tuning this value, you may be able to reduce the poll interval, which will reduce the
 *     impact of group rebalancing.</li>
 * </ol>
 * <p>
 * For use cases where message processing time varies unpredictably, neither of these options may be sufficient.
 * The recommended way to handle these cases is to move message processing to another thread, which allows
 * the consumer to continue calling {@link #poll(long) poll} while the processor is still working. Some care must be taken
 * to ensure that committed offsets do not get ahead of the actual position. Typically, you must disable automatic
 * commits and manually commit processed offsets for records only after the thread has finished handling them
 * (depending on the delivery semantics you need). Note also that you will need to {@link #pause(Collection) pause}
 * the partition so that no new records are received from poll until after thread has finished handling those
 * previously returned.
 *
 * <h3>Usage Examples</h3>
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 *
 * <h4>Automatic Offset Committing</h4>
 * This example demonstrates a simple usage of Kafka's consumer api that relying on automatic offset committing.
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.put(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.put(&quot;group.id&quot;, &quot;test&quot;);
 *     props.put(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     props.put(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     props.put(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.put(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(100);
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 *
 * The connection to the cluster is bootstrapped by specifying a list of one or more brokers to contact using the
 * configuration <code>bootstrap.servers</code>. This list is just used to discover the rest of the brokers in the
 * cluster and need not be an exhaustive list of servers in the cluster (though you may want to specify more than one in
 * case there are servers down when the client is connecting).
 * <p>
 * Setting <code>enable.auto.commit</code> means that offsets are committed automatically with a frequency controlled by
 * the config <code>auto.commit.interval.ms</code>.
 * <p>
 * In this example the consumer is subscribing to the topics <i>foo</i> and <i>bar</i> as part of a group of consumers
 * called <i>test</i> as configured with <code>group.id</code>.
 * <p>
 * The deserializer settings specify how to turn bytes into objects. For example, by specifying string deserializers, we
 * are saying that our record's key and value will just be simple strings.
 *
 * <h4>Manual Offset Control</h4>
 *
 * Instead of relying on the consumer to periodically commit consumed offsets, users can also control when records
 * should be considered as consumed and hence commit their offsets. This is useful when the consumption of the
 * are coupled with some processing logic and hence a message should not be considered as consumed until it is completed processing.

 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.put(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.put(&quot;group.id&quot;, &quot;test&quot;);
 *     props.put(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     props.put(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.put(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     final int minBatchSize = 200;
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(100);
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             buffer.add(record);
 *         }
 *         if (buffer.size() &gt;= minBatchSize) {
 *             insertIntoDb(buffer);
 *             consumer.commitSync();
 *             buffer.clear();
 *         }
 *     }
 * </pre>
 *
 * In this example we will consume a batch of records and batch them up in memory. When we have enough records
 * batched, we will insert them into a database. If we allowed offsets to auto commit as in the previous example, records
 * would be considered consumed after they were returned to the user in {@link #poll(long) poll}. It would then be possible
 * for our process to fail after batching the records, but before they had been inserted into the database.
 * <p>
 * To avoid this, we will manually commit the offsets only after the corresponding records have been inserted into the
 * database. This gives us exact control of when a record is considered consumed. This raises the opposite possibility:
 * the process could fail in the interval after the insert into the database but before the commit (even though this
 * would likely just be a few milliseconds, it is a possibility). In this case the process that took over consumption
 * would consume from last committed offset and would repeat the insert of the last batch of data. Used in this way
 * Kafka provides what is often called "at-least-once" delivery guarantees, as each record will likely be delivered one
 * time but in failure cases could be duplicated.
 * <p>
 * <b>Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that
 * you must consume all data returned from each call to {@link #poll(long)} before any subsequent calls, or before
 * {@link #close() closing} the consumer. If you fail to do either of these, it is possible for the committed offset
 * to get ahead of the consumed position, which results in missing records. The advantage of using manual offset
 * control is that you have direct control over when a record is considered "consumed."</b>
 * <p>
 * The above example uses {@link #commitSync() commitSync} to mark all received records as committed. In some cases
 * you may wish to have even finer control over which records have been committed by specifying an offset explicitly.
 * In the example below we commit offset after we finish handling the records in each partition.
 * <p>
 * <pre>
 *     try {
 *         while(running) {
 *             ConsumerRecords&lt;String, String&gt; records = consumer.poll(Long.MAX_VALUE);
 *             for (TopicPartition partition : records.partitions()) {
 *                 List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *                 for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *                     System.out.println(record.offset() + &quot;: &quot; + record.value());
 *                 }
 *                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
 *                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
 *             }
 *         }
 *     } finally {
 *       consumer.close();
 *     }
 * </pre>
 *
 * <b>Note: The committed offset should always be the offset of the next message that your application will read.</b>
 * Thus, when calling {@link #commitSync(Map) commitSync(offsets)} you should add one to the offset of the last message processed.
 *
 * <h4><a name="manualassignment">Manual Partition Assignment</a></h4>
 *
 * In the previous examples, we subscribed to the topics we were interested in and let Kafka dynamically assign a
 * fair share of the partitions for those topics based on the active consumers in the group. However, in
 * some cases you may need finer control over the specific partitions that are assigned. For example:
 * <p>
 * <ul>
 * <li>If the process is maintaining some kind of local state associated with that partition (like a
 * local on-disk key-value store), then it should only get records for the partition it is maintaining on disk.
 * <li>If the process itself is highly available and will be restarted if it fails (perhaps using a
 * cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework). In
 * this case there is no need for Kafka to detect the failure and reassign the partition since the consuming process
 * will be restarted on another machine.
 * </ul>
 * <p>
 * To use this mode, instead of subscribing to the topic using {@link #subscribe(Collection) subscribe}, you just call
 * {@link #assign(Collection)} with the full list of partitions that you want to consume.
 *
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.assign(Arrays.asList(partition0, partition1));
 * </pre>
 *
 * Once assigned, you can call {@link #poll(long) poll} in a loop, just as in the preceding examples to consume
 * records. The group that the consumer specifies is still used for committing offsets, but now the set of partitions
 * will only change with another call to {@link #assign(Collection) assign}. Manual partition assignment does
 * not use group coordination, so consumer failures will not cause assigned partitions to be rebalanced. Each consumer
 * acts independently even if it shares a groupId with another consumer. To avoid offset commit conflicts, you should
 * usually ensure that the groupId is unique for each consumer instance.
 * <p>
 * Note that it isn't possible to mix manual partition assignment (i.e. using {@link #assign(Collection) assign})
 * with dynamic partition assignment through topic subscription (i.e. using {@link #subscribe(Collection) subscribe}).
 *
 * <h4><a name="rebalancecallback">Storing Offsets Outside Kafka</h4>
 *
 * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a store of its own
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
 * <p>
 * Each record comes with its own offset, so to manage your own offset you just need to do the following:
 *
 * <ul>
 * <li>Configure <code>enable.auto.commit=false</code>
 * <li>Use the offset provided with each {@link ConsumerRecord} to save your position.
 * <li>On restart restore the position of the consumer using {@link #seek(TopicPartition, long)}.
 * </ul>
 *
 * <p>
 * This type of usage is simplest when the partition assignment is also done manually (this would be likely in the
 * search index use case described above). If the partition assignment is done automatically special care is
 * needed to handle the case where partition assignments change. This can be done by providing a
 * {@link ConsumerRebalanceListener} instance in the call to {@link #subscribe(Collection, ConsumerRebalanceListener)}
 * and {@link #subscribe(Pattern, ConsumerRebalanceListener)}.
 * For example, when partitions are taken from a consumer the consumer will want to commit its offset for those partitions by
 * implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}. When partitions are assigned to a
 * consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer
 * to that position by implementing {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}.
 * <p>
 * Another common use for {@link ConsumerRebalanceListener} is to flush any caches the application maintains for
 * partitions that are moved elsewhere.
 *
 * <h4>Controlling The Consumer's Position</h4>
 *
 * In most use cases the consumer will simply consume records from beginning to end, periodically committing its
 * position (either automatically or manually). However Kafka allows the consumer to manually control its position,
 * moving forward or backwards in a partition at will. This means a consumer can re-consume older records, or skip to
 * the most recent records without actually consuming the intermediate records.
 * <p>
 * There are several instances where manually controlling the consumer's position can be useful.
 * <p>
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not
 * attempt to catch up processing all records, but rather just skip to the most recent records.
 * <p>
 * Another use case is for a system that maintains local state as described in the previous section. In such a system
 * the consumer will want to initialize its position on start-up to whatever is contained in the local store. Likewise
 * if the local state is destroyed (say because the disk is lost) the state may be recreated on a new machine by
 * re-consuming all the data and recreating the state (assuming that Kafka is retaining sufficient history).
 * <p>
 * Kafka allows specifying the position using {@link #seek(TopicPartition, long)} to specify the new position. Special
 * methods for seeking to the earliest and latest offset the server maintains are also available (
 * {@link #seekToBeginning(Collection)} and {@link #seekToEnd(Collection)} respectively).
 *
 * <h4>Consumption Flow Control</h4>
 *
 * If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time,
 * effectively giving these partitions the same priority for consumption. However in some cases consumers may want to
 * first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions
 * when these partitions have few or no data to consume.
 *
 * <p>
 * One of such cases is stream processing, where processor fetches from two topics and performs the join on these two streams.
 * When one of the topics is long lagging behind the other, the processor would like to pause fetching from the ahead topic
 * in order to get the lagging stream to catch up. Another example is bootstraping upon consumer starting up where there are
 * a lot of history data to catch up, the applications usually want to get the latest data on some of the topics before consider
 * fetching other topics.
 *
 * <p>
 * Kafka supports dynamic controlling of consumption flows by using {@link #pause(Collection)} and {@link #resume(Collection)}
 * to pause the consumption on the specified assigned partitions and resume the consumption
 * on the specified paused partitions respectively in the future {@link #poll(long)} calls.
 *
 * <h3><a name="multithreaded">Multi-threaded Processing</a></h3>
 *
 * The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized. Un-synchronized access will result in {@link ConcurrentModificationException}.
 *
 * <p>
 * The only exception to this rule is {@link #wakeup()}, which can safely be used from an external thread to
 * interrupt an active operation. In this case, a {@link org.apache.kafka.common.errors.WakeupException} will be
 * thrown from the thread blocking on the operation. This can be used to shutdown the consumer from another thread.
 * The following snippet shows the typical pattern:
 *
 * <pre>
 * public class KafkaConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaConsumer consumer;
 *
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(10000);
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
 *
 * <p>
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * We have intentionally avoided implementing a particular threading model for processing. This leaves several
 * options for implementing multi-threaded processing of records.
 *
 * <h4>1. One Consumer Per Thread</h4>
 *
 * A simple option is to give each thread its own consumer instance. Here are the pros and cons of this approach:
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
 * There are many possible variations on this approach. For example each processor thread can have its own queue, and
 * the consumer threads can hash into these queues using the TopicPartition to ensure in-order consumption and simplify
 * commit.
 *
 */
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final long NO_CURRENT_THREAD = -1L;
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.consumer";

    private final String clientId;
    private final ConsumerCoordinator coordinator;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Fetcher<K, V> fetcher;
    private final ConsumerInterceptors<K, V> interceptors;

    private final Time time;
    private final ConsumerNetworkClient client;
    private final Metrics metrics;
    private final SubscriptionState subscriptions;
    private final Metadata metadata;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing KafkaConsumer
    // and is used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);

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
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
            keyDeserializer,
            valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     *
     * @param properties The consumer configuration properties
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
             keyDeserializer,
             valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    private KafkaConsumer(ConsumerConfig config,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer) {
        try {
            log.debug("Starting the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            int sessionTimeOutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
            int fetchMaxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
            if (this.requestTimeoutMs <= sessionTimeOutMs || this.requestTimeoutMs <= fetchMaxWaitMs)
                throw new ConfigException(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG + " should be greater than " + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG + " and " + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
            this.time = new SystemTime();

            String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "consumer-" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;
            Map<String, String> metricsTags = new LinkedHashMap<>();
            metricsTags.put("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .tags(metricsTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            // load interceptors and make sure they get clientId
            Map<String, Object> userProvidedConfigs = config.originals();
            userProvidedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            List<ConsumerInterceptor<K, V>> interceptorList = (List) (new ConsumerConfig(userProvidedConfigs)).getConfiguredInstances(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ConsumerInterceptor.class);
            this.interceptors = interceptorList.isEmpty() ? null : new ConsumerInterceptors<>(interceptorList);
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
                this.keyDeserializer.configure(config.originals(), true);
            } else {
                config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
                this.valueDeserializer.configure(config.originals(), false);
            } else {
                config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
                this.valueDeserializer = valueDeserializer;
            }
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keyDeserializer, valueDeserializer, reporters, interceptorList);
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG), false, clusterResourceListeners);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            this.metadata.update(Cluster.bootstrap(addresses), 0);
            String metricGrpPrefix = "consumer";
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder),
                    this.metadata,
                    clientId,
                    100, // a fixed large enough value will suffice
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), time);
            this.client = new ConsumerNetworkClient(netClient, metadata, time, retryBackoffMs,
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
            this.subscriptions = new SubscriptionState(offsetResetStrategy);
            List<PartitionAssignor> assignors = config.getConfiguredInstances(
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    PartitionAssignor.class);
            this.coordinator = new ConsumerCoordinator(this.client,
                    config.getString(ConsumerConfig.GROUP_ID_CONFIG),
                    config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                    config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                    config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG),
                    assignors,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricGrpPrefix,
                    this.time,
                    retryBackoffMs,
                    new ConsumerCoordinator.DefaultOffsetCommitCallback(),
                    config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
                    config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                    this.interceptors,
                    config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG));
            this.fetcher = new Fetcher<>(this.client,
                    config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                    config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                    config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                    this.keyDeserializer,
                    this.valueDeserializer,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricGrpPrefix,
                    this.time,
                    this.retryBackoffMs);

            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId);

            log.debug("Kafka consumer created");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            close(true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // visible for testing
    KafkaConsumer(String clientId,
                  ConsumerCoordinator coordinator,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  Fetcher<K, V> fetcher,
                  ConsumerInterceptors<K, V> interceptors,
                  Time time,
                  ConsumerNetworkClient client,
                  Metrics metrics,
                  SubscriptionState subscriptions,
                  Metadata metadata,
                  long retryBackoffMs,
                  long requestTimeoutMs) {
        this.clientId = clientId;
        this.coordinator = coordinator;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.fetcher = fetcher;
        this.interceptors = interceptors;
        this.time = time;
        this.client = client;
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {
        acquire();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.assignedPartitions()));
        } finally {
            release();
        }
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     * @return The set of topics currently subscribed to
     */
    public Set<String> subscription() {
        acquire();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger -
     * <ul>
     * <li>Number of partitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that this listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquire();
        try {
            if (topics == null) {
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            } else if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }
                log.debug("Subscribed to topic(s): {}", Utils.join(topics, ", "));
                this.subscriptions.subscribe(new HashSet<>(topics), listener);
                metadata.setTopics(subscriptions.groupSubscription());
            }
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a noop listener. If you need the ability to either seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also prefer to provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     */
    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions. The pattern matching will be done periodically against topics
     * existing at the time of check.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that
     * belong to a particular group and will trigger a rebalance operation if one of the
     * following events trigger -
     * <ul>
     * <li>Number of partitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     *
     * @param pattern Pattern to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If pattern is null
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        acquire();
        try {
            if (pattern == null)
                throw new IllegalArgumentException("Topic pattern to subscribe to cannot be null");
            log.debug("Subscribed to pattern: {}", pattern);
            this.subscriptions.subscribe(pattern, listener);
            this.metadata.needMetadataForAllTopics(true);
            this.metadata.requestUpdate();
            this.coordinator.updatePatternSubscription(metadata.fetch());
        } finally {
            release();
        }
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)}. This
     * also clears any partitions directly assigned through {@link #assign(Collection)}.
     */
    public void unsubscribe() {
        acquire();
        try {
            log.debug("Unsubscribed all topics or patterns and assigned partitions");
            this.subscriptions.unsubscribe();
            this.coordinator.maybeLeaveGroup();
            this.metadata.needMetadataForAllTopics(false);
        } finally {
            release();
        }
    }

    /**
     * Manually assign a list of partition to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     *
     * If the given list of topic partition is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     *
     * @param partitions The list of partitions to assign this consumer
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquire();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                this.unsubscribe();
            } else {
                Set<String> topics = new HashSet<>();
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                    topics.add(topic);
                }

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                this.coordinator.maybeAutoCommitOffsetsNow();

                log.debug("Subscribed to partition(s): {}", Utils.join(partitions, ", "));
                this.subscriptions.assignFromUser(new HashSet<>(partitions));
                metadata.setTopics(topics);
            }
        } finally {
            release();
        }
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *            If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *            Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     */
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        acquire();
        try {
            if (timeout < 0)
                throw new IllegalArgumentException("Timeout must not be negative");

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment())
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");

            // poll for new data until the timeout expires
            long start = time.milliseconds();
            long remaining = timeout;
            do {
                Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollOnce(remaining);
                if (!records.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    fetcher.sendFetches();
                    client.pollNoWakeup();

                    if (this.interceptors == null)
                        return new ConsumerRecords<>(records);
                    else
                        return this.interceptors.onConsume(new ConsumerRecords<>(records));
                }

                long elapsed = time.milliseconds() - start;
                remaining = timeout - elapsed;
            } while (remaining > 0);

            return ConsumerRecords.empty();
        } finally {
            release();
        }
    }

    /**
     * Do one round of polling. In addition to checking for new data, this does any needed offset commits
     * (if auto-commit is enabled), and offset resets (if an offset reset policy is defined).
     * @param timeout The maximum time to block in the underlying call to {@link ConsumerNetworkClient#poll(long)}.
     * @return The fetched records (may be empty)
     */
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {
        coordinator.poll(time.milliseconds());

        // fetch positions if we have partitions we're subscribed to that we
        // don't know the offset for
        if (!subscriptions.hasAllFetchPositions())
            updateFetchPositions(this.subscriptions.missingFetchPositions());

        // if data is available already, return it immediately
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
        if (!records.isEmpty())
            return records;

        // send any new fetches (won't resend pending fetches)
        fetcher.sendFetches();

        long now = time.milliseconds();
        long pollTimeout = Math.min(coordinator.timeToNextPoll(now), timeout);

        client.poll(pollTimeout, now, new PollCondition() {
            @Override
            public boolean shouldBlock() {
                // since a fetch might be completed by the background thread, we need this poll condition
                // to ensure that we do not block unnecessarily in poll()
                return !fetcher.hasCompletedFetches();
            }
        });

        // after the long poll, we should check whether the group needs to rebalance
        // prior to returning data so that the group can stabilize faster
        if (coordinator.needRejoin())
            return Collections.emptyMap();

        return fetcher.fetchedRecords();
    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller).
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same groupId which is using group management.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the committed offset is invalid).
     */
    @Override
    public void commitSync() {
        acquire();
        try {
            commitSync(subscriptions.allConsumed());
        } finally {
            release();
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller).
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same groupId which is using group management.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the committed offset is invalid).
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        acquire();
        try {
            coordinator.commitOffsetsSync(offsets);
        } finally {
            release();
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and partition.
     * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     *
     * @param callback Callback to invoke when the commit completes
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        acquire();
        try {
            commitAsync(subscriptions.allConsumed(), callback);
        } finally {
            release();
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     *
     * @param offsets A map of offsets by partition with associate metadata. This map will be copied internally, so it
     *                is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquire();
        try {
            log.debug("Committing offsets: {} ", offsets);
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }
        acquire();
        try {
            log.debug("Seeking to offset {} for partition {}", offset, partition);
            this.subscriptions.seek(partition, offset);
        } finally {
            release();
        }
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are called.
     * If no partition is provided, seek to the first offset for all of the currently assigned partitions.
     */
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        acquire();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            for (TopicPartition tp : parts) {
                log.debug("Seeking to beginning of partition {}", tp);
                subscriptions.needOffsetReset(tp, OffsetResetStrategy.EARLIEST);
            }
        } finally {
            release();
        }
    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
     * final offset in all partitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are called.
     * If no partition is provided, seek to the final offset for all of the currently assigned partitions.
     */
    public void seekToEnd(Collection<TopicPartition> partitions) {
        acquire();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            for (TopicPartition tp : parts) {
                log.debug("Seeking to end of partition {}", tp);
                subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);
            }
        } finally {
            release();
        }
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     *
     * @param partition The partition to get the position for
     * @return The offset
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    public long position(TopicPartition partition) {
        acquire();
        try {
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
            Long offset = this.subscriptions.position(partition);
            if (offset == null) {
                updateFetchPositions(Collections.singleton(partition));
                offset = this.subscriptions.position(partition);
            }
            return offset;
        } finally {
            release();
        }
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call may block to do a remote call if the partition in question isn't assigned to this consumer or if the
     * consumer hasn't yet initialized its cache of committed offsets.
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        acquire();
        try {
            OffsetAndMetadata committed;
            if (subscriptions.isAssigned(partition)) {
                committed = this.subscriptions.committed(partition);
                if (committed == null) {
                    coordinator.refreshCommittedOffsetsIfNeeded();
                    committed = this.subscriptions.committed(partition);
                }
            } else {
                Map<TopicPartition, OffsetAndMetadata> offsets = coordinator.fetchCommittedOffsets(Collections.singleton(partition));
                committed = offsets.get(partition);
            }

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
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the configured request timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        acquire();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (parts != null)
                return parts;

            Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(new MetadataRequest(Collections.singletonList(topic)), requestTimeoutMs);
            return topicMetadata.get(topic);
        } finally {
            release();
        }
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.

     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the configured request timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        acquire();
        try {
            return fetcher.getAllTopicMetadata(requestTimeoutMs);
        } finally {
            release();
        }
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(long)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     * @param partitions The partitions which should be paused
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquire();
        try {
            for (TopicPartition partition: partitions) {
                log.debug("Pausing partition {}", partition);
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(long)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     * @param partitions The partitions which should be resumed
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquire();
        try {
            for (TopicPartition partition: partitions) {
                log.debug("Resuming partition {}", partition);
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused partitions
     */
    @Override
    public Set<TopicPartition> paused() {
        acquire();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * Notice that this method may block indefinitely if the partition does not exist.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws IllegalArgumentException if the target timestamp is negative.
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
            // OffsetAndTimestamp is always positive.
            if (entry.getValue() < 0)
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
        }
        return fetcher.getOffsetsByTimes(timestampsToSearch, requestTimeoutMs);
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * Notice that this method may block indefinitely if the partition does not exist.
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return fetcher.beginningOffsets(partitions, requestTimeoutMs);
    }

    /**
     * Get the last offset for the given partitions. The last offset of a partition is the offset of the upcoming
     * message, i.e. the offset of the last available message + 1.
     * <p>
     * Notice that this method may block indefinitely if the partition does not exist.
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return fetcher.endOffsets(partitions, requestTimeoutMs);
    }

    /**
     * Close the consumer, waiting indefinitely for any needed cleanup. If auto-commit is enabled, this
     * will commit the current offsets. Note that {@link #wakeup()} cannot be use to interrupt close.
     */
    @Override
    public void close() {
        acquire();
        try {
            close(false);
        } finally {
            release();
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     */
    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private void close(boolean swallowException) {
        log.trace("Closing the Kafka consumer.");
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        this.closed = true;
        ClientUtils.closeQuietly(coordinator, "coordinator", firstException);
        ClientUtils.closeQuietly(interceptors, "consumer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "consumer metrics", firstException);
        ClientUtils.closeQuietly(client, "consumer network client", firstException);
        ClientUtils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
        ClientUtils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId);
        log.debug("The Kafka consumer has closed.");
        if (firstException.get() != null && !swallowException) {
            throw new KafkaException("Failed to close kafka consumer", firstException.get());
        }
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @param partitions The partitions that needs updating fetch positions
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     */
    private void updateFetchPositions(Set<TopicPartition> partitions) {
        // lookup any positions for partitions which are awaiting reset (which may be the
        // case if the user called seekToBeginning or seekToEnd. We do this check first to
        // avoid an unnecessary lookup of committed offsets (which typically occurs when
        // the user is manually assigning partitions and managing their own offsets).
        fetcher.resetOffsetsIfNeeded(partitions);

        if (!subscriptions.hasAllFetchPositions()) {
            // if we still don't have offsets for all partitions, then we should either seek
            // to the last committed position or reset using the auto reset policy

            // first refresh commits for all assigned partitions
            coordinator.refreshCommittedOffsetsIfNeeded();

            // then do any offset lookups in case some positions are not known
            fetcher.updateFetchPositions(partitions);
        }
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
