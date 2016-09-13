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

package org.apache.kafka.common;

/**
 * A callback interface that users can implement when they wish to get notified about changes in the
 * Cluster metadata.
 * <p>
 * Users who need access to cluster metadata in interceptors, metric reporters, serializers and deserializers
 * can implement this interface. The order of method calls for each of these types is described below
 * <p>
 * {@link org.apache.kafka.clients.producer.ProducerInterceptor} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be called after {@link org.apache.kafka.clients.producer.ProducerInterceptor#onSend(ProducerRecord)}
 * but before {@link org.apache.kafka.clients.producer.ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception)} .
 * <p>
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be called before {@link org.apache.kafka.clients.consumer.ConsumerInterceptor#onConsume(ConsumerRecords)}
 * <p>
 * {@link org.apache.kafka.common.serialization.Serializer} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be called before {@link org.apache.kafka.common.serialization.Serializer#serialize(String, Object)}
 * <p>
 * {@link org.apache.kafka.common.serialization.Deserializer} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be called before {@link org.apache.kafka.common.serialization.Deserializer#deserialize(String, byte[])}
 * <p>
 * {@link org.apache.kafka.common.metrics.MetricsReporter} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be called during the bootup of the Kafka broker. The reporter may receive metric events from the network layer before this method is called.
 * <p>
 * KafkaMetricsReporter : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be called during the bootup of the Kafka broker. The reporter may receive metric events from the network layer before this method is called.
 */
public interface ClusterResourceListener {
    /**
     * A callback method that a user can implement to get updates for {@link ClusterResource}.
     * @param clusterResource cluster metadata
     */
    void onUpdate(ClusterResource clusterResource);
}
