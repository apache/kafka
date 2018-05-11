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
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.streams.Topology;

/**
 * Determine how records are distributed among the partitions in a Kafka topic. If not specified, the underlying producer's
 * {@link DefaultPartitioner} will be used to determine the partition.
 * <p>
 * Kafka topics are divided into one or more <i>partitions</i>. Since each partition must fit on the servers that host it, so
 * using multiple partitions allows the topic to scale beyond a size that will fit on a single machine. Partitions also enable you
 * to use multiple instances of your topology to process in parallel all of the records on the topology's source topics.
 * <p>
 * When a topology is instantiated, each of its sources are assigned a subset of that topic's partitions. That means that only
 * those processors in that topology instance will consume the records from those partitions. In many cases, Kafka Streams will
 * automatically manage these instances, and adjust when new topology instances are added or removed.
 * <p>
 * Some topologies, though, need more control over which records appear in each partition. For example, some topologies that have
 * stateful processors may want all records within a range of keys to always be delivered to and handled by the same topology instance.
 * An upstream topology producing records to that topic can use a custom <i>stream partitioner</i> to precisely and consistently
 * determine to which partition each record should be written.
 * <p>
 * To do this, create a <code>StreamPartitioner</code> implementation, and when you build your topology specify that custom partitioner
 * when {@link Topology#addSink(String, String, org.apache.kafka.common.serialization.Serializer, org.apache.kafka.common.serialization.Serializer, StreamPartitioner, String...) adding a sink}
 * for that topic.
 * <p>
 * All StreamPartitioner implementations should be stateless and a pure function so they can be shared across topic and sink nodes.
 * 
 * @param <K> the type of keys
 * @param <V> the type of values
 * @see Topology#addSink(String, String, org.apache.kafka.common.serialization.Serializer,
 *      org.apache.kafka.common.serialization.Serializer, StreamPartitioner, String...)
 * @see Topology#addSink(String, String, StreamPartitioner, String...)
 */
public interface StreamPartitioner<K, V> {

    /**
     * Determine the partition number for a record with the given key and value and the current number of partitions.
     * 
     * @param key the key of the record
     * @param value the value of the record
     * @param numPartitions the total number of partitions
     * @return an integer between 0 and {@code numPartitions-1}, or {@code null} if the default partitioning logic should be used
     */
    Integer partition(K key, V value, int numPartitions);
}
