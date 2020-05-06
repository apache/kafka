/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.MetricName;


/**
 * KafkaProducer实现接口，支持try-with-resource释放资源
 * The interface for the {@link KafkaProducer}
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer<K, V> extends Closeable {

    /**
     * 异步发送给定记录，并返回将来将最终包含响应信息的future。
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     *  实际是将消息放入Recordaccumulator暂存，等待发送
     * @param record The record to send 发送的数据
     * @return A future which will eventually contain the response information
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * 发送一个记录并调用一个给定的回调让这个记录已经被服务器确认时
     * Send a record and invoke the given callback when the record has been acknowledged by the server
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
    
    /**
     * 刷新操作，等到RecordAccumulator中所有消息发送完成，在刷新完成之前会阻塞调用的线程。
     * 刷新生产者的所有累积记录。阻塞直到所有发送完成。
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    public void flush();

    /**
     * 获取给定主题的分区列表，以进行自定义分区分配。分区元数据会随着时间的变化，因此不应缓存此列表。
     * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
     * over time so this list should not be cached.
     * 在KafkaProducer中维护了一个Metadata对象用于存储Kafka集群的元数据，Metadata中的元数据会定期更新。
     * partitionsFor()方法负责从Metadata中获取指定Topic中的分区信息。
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * 返回这个生产者指标
     * Return a map of metrics maintained by the producer
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * 关闭这个生产者。设置close标志，等待RecordAccumulator中的消息清空，关闭Sender线程。
     * Close this producer
     */
    public void close();

    /**
     * 尝试在指定的超时时间内完全关闭生产者。如果关闭未在超时内完成，则使所有未决的发送请求失败，并强制关闭生产者。
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    public void close(long timeout, TimeUnit unit);

}
