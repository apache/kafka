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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ClientUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * A Kafka client that consumes records from a Kafka cluster.
 * <P>
 * The consumer is <i>thread safe</i> and should generally be shared among all threads for best performance.
 * <p>
 * The consumer is single threaded and multiplexes I/O over TCP connections to each of the brokers it
 * needs to communicate with. Failure to close the consumer after use will leak these resources.
 * <h3>Usage Examples</h3>
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Following are some examples to demonstrate the correct use of 
 * the available APIs. Each of the examples assumes the presence of a user implemented process() method that processes a given batch of messages
 * and returns the offset of the latest processed message per partition. Note that process() is not part of the consumer API and is only used as
 * a convenience method to demonstrate the different use cases of the consumer APIs. Here is a sample implementation of such a process() method.
 * <pre>
 * {@code
 * private Map<TopicPartition, Long> process(Map<String, ConsumerRecord<byte[], byte[]> records) {
 *     Map<TopicPartition, Long> processedOffsets = new HashMap<TopicPartition, Long>();
 *     for(Entry<String, ConsumerRecords<byte[], byte[]>> recordMetadata : records.entrySet()) {
 *          List<ConsumerRecord<byte[], byte[]>> recordsPerTopic = recordMetadata.getValue().records();
 *          for(int i = 0;i < recordsPerTopic.size();i++) {
 *               ConsumerRecord<byte[], byte[]> record = recordsPerTopic.get(i);
 *               // process record
 *               try {
 *               	processedOffsets.put(record.topicAndpartition(), record.offset());
 *               } catch (Exception e) {
 *               	e.printStackTrace();
 *               }               
 *          }
 *     }
 *     return processedOffsets; 
 * }
 * }
 * </pre>
 * <p>
 * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality for automatic consumer load 
 * balancing and failover. This example assumes that the offsets are stored in Kafka and are automatically committed periodically, 
 * as controlled by the auto.commit.interval.ms config
 * <pre>
 * {@code  
 * Properties props = new Properties();
 * props.put("metadata.broker.list", "localhost:9092");
 * props.put("group.id", "test");
 * props.put("session.timeout.ms", "1000");
 * props.put("enable.auto.commit", "true");
 * props.put("auto.commit.interval.ms", "10000");
 * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
 * consumer.subscribe("foo", "bar");
 * boolean isRunning = true;
 * while(isRunning) {
 *   Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
 *   process(records);
 * }
 * consumer.close();
 * }
 * </pre>
 * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality for automatic consumer load 
 * balancing and failover. This example assumes that the offsets are stored in Kafka and are manually committed using 
 * the commit(boolean) API. This example also demonstrates rewinding the consumer's offsets if processing of the consumed
 * messages fails. Note that this method of rewinding offsets using {@link #seek(Map) seek(offsets)} is only useful for rewinding the offsets
 * of the current consumer instance. As such, this will not trigger a rebalance or affect the fetch offsets for the other consumer instances.
 * <pre>
 * {@code  
 * Properties props = new Properties();
 * props.put("metadata.broker.list", "localhost:9092");
 * props.put("group.id", "test");
 * props.put("session.timeout.ms", "1000");
 * props.put("enable.auto.commit", "false");
 * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
 * consumer.subscribe("foo", "bar");
 * int commitInterval = 100;
 * int numRecords = 0;
 * boolean isRunning = true;
 * Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
 * while(isRunning) {
 *     Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
 *     try {
 *         Map<TopicPartition, Long> lastConsumedOffsets = process(records);
 *         consumedOffsets.putAll(lastConsumedOffsets);
 *         numRecords += records.size();
 *         // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
 *         if(numRecords % commitInterval == 0) 
 *           consumer.commit(false);
 *     } catch(Exception e) {
 *         try {
 *             // rewind consumer's offsets for failed partitions
 *             // assume failedPartitions() returns the list of partitions for which the processing of the last batch of messages failed
 *             List<TopicPartition> failedPartitions = failedPartitions();   
 *             Map<TopicPartition, Long> offsetsToRewindTo = new HashMap<TopicPartition, Long>();
 *             for(TopicPartition failedPartition : failedPartitions) {
 *                 // rewind to the last consumed offset for the failed partition. Since process() failed for this partition, the consumed offset
 *                 // should still be pointing to the last successfully processed offset and hence is the right offset to rewind consumption to.
 *                 offsetsToRewindTo.put(failedPartition, consumedOffsets.get(failedPartition));
 *             }
 *             // seek to new offsets only for partitions that failed the last process()
 *             consumer.seek(offsetsToRewindTo);
 *         } catch(Exception e) {  break; } // rewind failed
 *     }
 * }         
 * consumer.close();
 * }
 * </pre>
 * <p>
 * This example demonstrates how to rewind the offsets of the entire consumer group. It is assumed that the user has chosen to use Kafka's 
 * group management functionality for automatic consumer load balancing and failover. This example also assumes that the offsets are stored in 
 * Kafka. If group management is used, the right place to systematically rewind offsets for <i>every</i> consumer instance is inside the 
 * ConsumerRebalanceCallback. The onPartitionsAssigned callback is invoked after the consumer is assigned a new set of partitions on rebalance 
 * <i>and</i> before the consumption restarts post rebalance. This is the right place to supply the newly rewound offsets to the consumer. It 
 * is recommended that if you foresee the requirement to ever reset the consumer's offsets in the presence of group management, that you 
 * always configure the consumer to use the ConsumerRebalanceCallback with a flag that protects whether or not the offset rewind logic is used.
 * This method of rewinding offsets is useful if you notice an issue with your message processing after successful consumption and offset commit.
 * And you would like to rewind the offsets for the entire consumer group as part of rolling out a fix to your processing logic. In this case,
 * you would configure each of your consumer instances with the offset rewind configuration flag turned on and bounce each consumer instance 
 * in a rolling restart fashion. Each restart will trigger a rebalance and eventually all consumer instances would have rewound the offsets for 
 * the partitions they own, effectively rewinding the offsets for the entire consumer group.   
 * <pre>
 * {@code  
 * Properties props = new Properties();
 * props.put("metadata.broker.list", "localhost:9092");
 * props.put("group.id", "test");
 * props.put("session.timeout.ms", "1000");
 * props.put("enable.auto.commit", "false");
 * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
 *                                            props,
 *                                            new ConsumerRebalanceCallback() {
 *                                                boolean rewindOffsets = true;  // should be retrieved from external application config
 *                                                public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
 *                                                    Map<TopicPartition, Long> latestCommittedOffsets = consumer.committed(partitions);
 *                                                    if(rewindOffsets)
 *                                                        Map<TopicPartition, Long> newOffsets = rewindOffsets(latestCommittedOffsets, 100);
 *                                                    consumer.seek(newOffsets);
 *                                                }
 *                                                public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
 *                                                    consumer.commit(true);
 *                                                }
 *                                                // this API rewinds every partition back by numberOfMessagesToRewindBackTo messages 
 *                                                private Map<TopicPartition, Long> rewindOffsets(Map<TopicPartition, Long> currentOffsets,
 *                                                                                                long numberOfMessagesToRewindBackTo) {
 *                                                    Map<TopicPartition, Long> newOffsets = new HashMap<TopicPartition, Long>();
 *                                                    for(Map.Entry<TopicPartition, Long> offset : currentOffsets.entrySet()) 
 *                                                        newOffsets.put(offset.getKey(), offset.getValue() - numberOfMessagesToRewindBackTo);
 *                                                    return newOffsets;
 *                                                }
 *                                            });
 * consumer.subscribe("foo", "bar");
 * int commitInterval = 100;
 * int numRecords = 0;
 * boolean isRunning = true;
 * Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
 * while(isRunning) {
 *     Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
 *     Map<TopicPartition, Long> lastConsumedOffsets = process(records);
 *     consumedOffsets.putAll(lastConsumedOffsets);
 *     numRecords += records.size();
 *     // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
 *     if(numRecords % commitInterval == 0) 
 *         consumer.commit(consumedOffsets, true);
 * }
 * consumer.commit(true);
 * consumer.close();
 * }
 * </pre>
 * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality along with custom offset storage. 
 * In this example, the assumption made is that the user chooses to store the consumer offsets outside Kafka. This requires the user to 
 * plugin logic for retrieving the offsets from a custom store and provide the offsets to the consumer in the ConsumerRebalanceCallback
 * callback. The onPartitionsAssigned callback is invoked after the consumer is assigned a new set of partitions on rebalance <i>and</i>
 * before the consumption restarts post rebalance. This is the right place to supply offsets from a custom store to the consumer.
 * <p>
 * Similarly, the user would also be required to plugin logic for storing the consumer's offsets to a custom store. The onPartitionsRevoked 
 * callback is invoked right after the consumer has stopped fetching data and before the partition ownership changes. This is the right place 
 * to commit the offsets for the current set of partitions owned by the consumer.  
 * <pre>
 * {@code  
 * Properties props = new Properties();
 * props.put("metadata.broker.list", "localhost:9092");
 * props.put("group.id", "test");
 * props.put("session.timeout.ms", "1000");
 * props.put("enable.auto.commit", "false"); // since enable.auto.commit only applies to Kafka based offset storage
 * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
 *                                            props,
 *                                            new ConsumerRebalanceCallback() {
 *                                                public void onPartitionsAssigned(Consumer<?,?> consumer, Collection<TopicPartition> partitions) {
 *                                                    Map<TopicPartition, Long> lastCommittedOffsets = getLastCommittedOffsetsFromCustomStore(partitions);
 *                                                    consumer.seek(lastCommittedOffsets);
 *                                                }
 *                                                public void onPartitionsRevoked(Consumer<?,?> consumer, Collection<TopicPartition> partitions) {
 *                                                    Map<TopicPartition, Long> offsets = getLastConsumedOffsets(partitions);
 *                                                    commitOffsetsToCustomStore(offsets); 
 *                                                }
 *                                                // following APIs should be implemented by the user for custom offset management
 *                                                private Map<TopicPartition, Long> getLastCommittedOffsetsFromCustomStore(Collection<TopicPartition> partitions) {
 *                                                    return null;
 *                                                }
 *                                                private Map<TopicPartition, Long> getLastConsumedOffsets(Collection<TopicPartition> partitions) { return null; }
 *                                                private void commitOffsetsToCustomStore(Map<TopicPartition, Long> offsets) {}
 *                                            });
 * consumer.subscribe("foo", "bar");
 * int commitInterval = 100;
 * int numRecords = 0;
 * boolean isRunning = true;
 * Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
 * while(isRunning) {
 *     Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
 *     Map<TopicPartition, Long> lastConsumedOffsets = process(records);
 *     consumedOffsets.putAll(lastConsumedOffsets);
 *     numRecords += records.size();
 *     // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
 *     if(numRecords % commitInterval == 0) 
 *         commitOffsetsToCustomStore(consumedOffsets);
 * }
 * consumer.commit(true);
 * consumer.close();
 * }
 * </pre>
 * This example demonstrates how the consumer can be used to subscribe to specific partitions of certain topics and consume upto the latest
 * available message for each of those partitions before shutting down. When used to subscribe to specific partitions, the user foregoes 
 * the group management functionality and instead relies on manually configuring the consumer instances to subscribe to a set of partitions.
 * This example assumes that the user chooses to use Kafka based offset storage. The user still has to specify a group.id to use Kafka 
 * based offset management. However, session.timeout.ms is not required since the Kafka consumer only does automatic failover when group 
 * management is used.
 * <pre>
 * {@code  
 * Properties props = new Properties();
 * props.put("metadata.broker.list", "localhost:9092");
 * props.put("group.id", "test");
 * props.put("enable.auto.commit", "true");
 * props.put("auto.commit.interval.ms", "10000");
 * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
 * // subscribe to some partitions of topic foo
 * TopicPartition partition0 = new TopicPartition("foo", 0);
 * TopicPartition partition1 = new TopicPartition("foo", 1);
 * TopicPartition[] partitions = new TopicPartition[2];
 * partitions[0] = partition0;
 * partitions[1] = partition1;
 * consumer.subscribe(partitions);
 * // find the last committed offsets for partitions 0,1 of topic foo
 * Map<TopicPartition, Long> lastCommittedOffsets = consumer.committed(Arrays.asList(partitions));
 * // seek to the last committed offsets to avoid duplicates
 * consumer.seek(lastCommittedOffsets);        
 * // find the offsets of the latest available messages to know where to stop consumption
 * Map<TopicPartition, Long> latestAvailableOffsets = consumer.offsetsBeforeTime(-2, Arrays.asList(partitions));
 * boolean isRunning = true;
 * Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
 * while(isRunning) {
 *     Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
 *     Map<TopicPartition, Long> lastConsumedOffsets = process(records);
 *     consumedOffsets.putAll(lastConsumedOffsets);
 *     for(TopicPartition partition : partitions) {
 *         if(consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
 *             isRunning = false;
 *         else
 *             isRunning = true;
 *     }
 * }
 * consumer.commit(true);
 * consumer.close();
 * }
 * </pre>
 * This example demonstrates how the consumer can be used to subscribe to specific partitions of certain topics and consume upto the latest
 * available message for each of those partitions before shutting down. When used to subscribe to specific partitions, the user foregoes 
 * the group management functionality and instead relies on manually configuring the consumer instances to subscribe to a set of partitions.
 * This example assumes that the user chooses to use custom offset storage.
 * <pre>
 * {@code  
 * Properties props = new Properties();
 * props.put("metadata.broker.list", "localhost:9092");
 * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
 * // subscribe to some partitions of topic foo
 * TopicPartition partition0 = new TopicPartition("foo", 0);
 * TopicPartition partition1 = new TopicPartition("foo", 1);
 * TopicPartition[] partitions = new TopicPartition[2];
 * partitions[0] = partition0;
 * partitions[1] = partition1;
 * consumer.subscribe(partitions);
 * Map<TopicPartition, Long> lastCommittedOffsets = getLastCommittedOffsetsFromCustomStore();
 * // seek to the last committed offsets to avoid duplicates
 * consumer.seek(lastCommittedOffsets);        
 * // find the offsets of the latest available messages to know where to stop consumption
 * Map<TopicPartition, Long> latestAvailableOffsets = consumer.offsetsBeforeTime(-2, Arrays.asList(partitions));
 * boolean isRunning = true;
 * Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
 * while(isRunning) {
 *     Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
 *     Map<TopicPartition, Long> lastConsumedOffsets = process(records);
 *     consumedOffsets.putAll(lastConsumedOffsets);
 *     // commit offsets for partitions 0,1 for topic foo to custom store
 *     commitOffsetsToCustomStore(consumedOffsets);
 *     for(TopicPartition partition : partitions) {
 *         if(consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
 *             isRunning = false;
 *         else
 *             isRunning = true;
 *     }            
 * }      
 * commitOffsetsToCustomStore(consumedOffsets);   
 * consumer.close();
 * }
 * </pre>
 */
public class KafkaConsumer<K,V> implements Consumer<K,V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final long metadataFetchTimeoutMs;
    private final long totalMemorySize;
    private final Metrics metrics;
    private final Set<String> subscribedTopics;
    private final Set<TopicPartition> subscribedPartitions;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * @param configs   The consumer configs
     */
    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration and a {@link ConsumerRebalanceCallback} 
     * implementation 
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * @param configs   The consumer configs
     * @param callback  A callback interface that the user can implement to manage customized offsets on the start and end of 
     *                  every rebalance operation.  
     */
    public KafkaConsumer(Map<String, Object> configs, ConsumerRebalanceCallback callback) {
        this(configs, callback, null, null);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, a {@link ConsumerRebalanceCallback}
     * implementation, a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * @param configs   The consumer configs
     * @param callback  A callback interface that the user can implement to manage customized offsets on the start and end of
     *                  every rebalance operation.
     * @param keyDeserializer  The deserializer for key that implements {@link Deserializer}. The configure() method won't
     *                         be called when the deserializer is passed in directly.
     * @param valueDeserializer  The deserializer for value that implements {@link Deserializer}. The configure() method
     *                           won't be called when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs, ConsumerRebalanceCallback callback, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
             callback, keyDeserializer, valueDeserializer);
    }

    private static Map<String, Object> addDeserializerToConfig(Map<String, Object> configs,
                                                               Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
        Map<String, Object> newConfigs = new HashMap<String, Object>();
        newConfigs.putAll(configs);
        if (keyDeserializer != null)
            newConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        if (keyDeserializer != null)
            newConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        return newConfigs;
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.      
     * Valid configuration strings are documented at {@link ConsumerConfig}
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration and a 
     * {@link ConsumerRebalanceCallback} implementation. 
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * @param properties The consumer configuration properties
     * @param callback   A callback interface that the user can implement to manage customized offsets on the start and end of 
     *                   every rebalance operation.  
     */
    public KafkaConsumer(Properties properties, ConsumerRebalanceCallback callback) {
        this(properties, callback, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration and a
     * {@link ConsumerRebalanceCallback} implementation, a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}
     * @param properties The consumer configuration properties
     * @param callback   A callback interface that the user can implement to manage customized offsets on the start and end of
     *                   every rebalance operation.
     * @param keyDeserializer  The deserializer for key that implements {@link Deserializer}. The configure() method won't
     *                         be called when the deserializer is passed in directly.
     * @param valueDeserializer  The deserializer for value that implements {@link Deserializer}. The configure() method
     *                           won't be called when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties, ConsumerRebalanceCallback callback, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
             callback, keyDeserializer, valueDeserializer);
    }

    private static Properties addDeserializerToConfig(Properties properties,
                                                      Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keyDeserializer != null)
            newProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        if (keyDeserializer != null)
            newProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        return newProperties;
    }

    private KafkaConsumer(ConsumerConfig config, ConsumerRebalanceCallback callback, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        log.trace("Starting the Kafka consumer");
        subscribedTopics = new HashSet<String>();
        subscribedPartitions = new HashSet<TopicPartition>();
        this.metrics = new Metrics(new MetricConfig(),
                                   Collections.singletonList((MetricsReporter) new JmxReporter("kafka.consumer.")),
                                   new SystemTime());
        this.metadataFetchTimeoutMs = config.getLong(ConsumerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
        this.totalMemorySize = config.getLong(ConsumerConfig.TOTAL_BUFFER_MEMORY_CONFIG);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

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

        config.logUnused();
        log.debug("Kafka consumer started");
    }

    /**
     * Incrementally subscribes to the given list of topics and uses the consumer's group management functionality
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular group and
     * will trigger a rebalance operation if one of the following events trigger -
     * <ul>
     * <li> Number of partitions change for any of the subscribed list of topics 
     * <li> Topic is created or deleted 
     * <li> An existing member of the consumer group dies 
     * <li> A new member is added to an existing consumer group via the join API 
     * </ul> 
     * @param topics A variable list of topics that the consumer wants to subscribe to
     */
    @Override
    public void subscribe(String... topics) {
        if(subscribedPartitions.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions is mutually exclusive");
        for(String topic:topics)
            subscribedTopics.add(topic);
        // TODO: trigger a rebalance operation
    }

    /**
     * Incrementally subscribes to a specific topic partition and does not use the consumer's group management functionality. As such,
     * there will be no rebalance operation triggered when group membership or cluster and topic metadata change.
     * <p>
     * @param partitions Partitions to incrementally subscribe to
     */
    @Override
    public void subscribe(TopicPartition... partitions) {
        if(subscribedTopics.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions is mutually exclusive");
        for(TopicPartition partition:partitions)
            subscribedPartitions.add(partition);
    }

    /**
     * Unsubscribe from the specific topics. This will trigger a rebalance operation and messages for this topic will not be returned 
     * from the next {@link #poll(long) poll()} onwards
     * @param topics Topics to unsubscribe from
     */
    public void unsubscribe(String... topics) {   
        // throw an exception if the topic was never subscribed to
        for(String topic:topics) {
            if(!subscribedTopics.contains(topic))
                throw new IllegalStateException("Topic " + topic + " was never subscribed to. subscribe(" + topic + ") should be called prior" +
                		" to unsubscribe(" + topic + ")");
            subscribedTopics.remove(topic);
        }
        // TODO trigger a rebalance operation
    }

    /**
     * Unsubscribe from the specific topic partitions. Messages for these partitions will not be returned from the next 
     * {@link #poll(long) poll()} onwards
     * @param partitions Partitions to unsubscribe from
     */
    public void unsubscribe(TopicPartition... partitions) {        
        // throw an exception if the partition was never subscribed to
        for(TopicPartition partition:partitions) {
            if(!subscribedPartitions.contains(partition))
                throw new IllegalStateException("Partition " + partition + " was never subscribed to. subscribe(new TopicPartition(" + 
                                                 partition.topic() + "," + partition.partition() + ") should be called prior" +
                                                " to unsubscribe(new TopicPartition(" + partition.topic() + "," + partition.partition() + ")");
            subscribedPartitions.remove(partition);                
        }
        // trigger a rebalance operation
    }
    
    /**
     * Fetches data for the topics or partitions specified using one of the subscribe APIs. It is an error to not have subscribed to
     * any topics or partitions before polling for data.
     * <p> 
     * The offset used for fetching the data is governed by whether or not {@link #seek(Map) seek(offsets)}
     * is used. If {@link #seek(Map) seek(offsets)} is used, it will use the specified offsets on startup and
     * on every rebalance, to consume data from that offset sequentially on every poll. If not, it will use the last checkpointed offset 
     * using {@link #commit(Map, boolean) commit(offsets, sync)} 
     * for the subscribed list of partitions.
     * @param timeout  The time, in milliseconds, spent waiting in poll if data is not available. If 0, waits indefinitely. Must not be negative
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     */
    @Override
    public Map<String, ConsumerRecords<K,V>> poll(long timeout) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Commits the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after every rebalance
     * and also on startup. As such, if you need to store offsets in anything other than Kafka, this API should not be used.
     * @param offsets The list of offsets per partition that should be committed to Kafka. 
     * @param sync If true, commit will block until the consumer receives an acknowledgment  
     * @return An {@link OffsetMetadata} object that contains the partition, offset and a corresponding error code. Returns null
     * if the sync flag is set to false.  
     */
    @Override
    public OffsetMetadata commit(Map<TopicPartition, Long> offsets, boolean sync) {
        throw new UnsupportedOperationException();
    }

    /**
     * Commits offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and 
     * partitions. 
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after every rebalance
     * and also on startup. As such, if you need to store offsets in anything other than Kafka, this API should not be used.
     * @param sync If true, commit will block until the consumer receives an acknowledgment  
     * @return An {@link OffsetMetadata} object that contains the partition, offset and a corresponding error code. Returns null
     * if the sync flag is set to false.
     */
    @Override
    public OffsetMetadata commit(boolean sync) {
        throw new UnsupportedOperationException();
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API is invoked
     * for the same partition more than once, the latest offset will be used on the next poll(). Note that you may lose data if this API is 
     * arbitrarily used in the middle of consumption, to reset the fetch offsets  
     */
    @Override
    public void seek(Map<TopicPartition, Long> offsets) {
    }

    /**
     * Returns the fetch position of the <i>next message</i> for the specified topic partition to be used on the next {@link #poll(long) poll()}
     * @param partitions Partitions for which the fetch position will be returned
     * @return The position from which data will be fetched for the specified partition on the next {@link #poll(long) poll()}
     */
    public Map<TopicPartition, Long> position(Collection<TopicPartition> partitions) {
        return null;
    }

    /**
     * Fetches the last committed offsets of partitions that the consumer currently consumes. This API is only relevant if Kafka based offset
     * storage is used. This API can be used in conjunction with {@link #seek(Map) seek(offsets)} to rewind consumption of data.  
     * @param partitions The list of partitions to return the last committed offset for
     * @return The list of offsets committed on the last {@link #commit(boolean) commit(sync)} 
     */
    @Override
    public Map<TopicPartition, Long> committed(Collection<TopicPartition> partitions) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /**
     * Fetches offsets before a certain timestamp. Note that the offsets returned are approximately computed and do not correspond to the exact
     * message at the given timestamp. As such, if the consumer is rewound to offsets returned by this API, there may be duplicate messages 
     * returned by the consumer. 
     * @param partitions The list of partitions for which the offsets are returned
     * @param timestamp The unix timestamp. Value -1 indicates earliest available timestamp. Value -2 indicates latest available timestamp. 
     * @return The offsets per partition before the specified timestamp.
     */
    public Map<TopicPartition, Long> offsetsBeforeTime(long timestamp, Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public void close() {
        log.trace("Closing the Kafka consumer.");
        subscribedTopics.clear();
        subscribedPartitions.clear();
        this.metrics.close();
        log.debug("The Kafka consumer has closed.");
    }
}
