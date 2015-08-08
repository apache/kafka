<<<<<<< HEAD
package org.apache.kafka.stream;

import io.confluent.streaming.kv.internals.RestoreFunc;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
=======
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

package org.apache.kafka.stream;

import org.apache.kafka.clients.processor.RecordCollector;
import org.apache.kafka.clients.processor.RestoreFunc;
import org.apache.kafka.clients.processor.StateStore;
import org.apache.kafka.clients.processor.internals.StreamingConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
<<<<<<< HEAD
import org.apache.kafka.stream.topology.Processor;
>>>>>>> added missing files
=======
>>>>>>> wip

import java.io.File;
import java.util.Map;

/**
 * KStreamContext is access to the system resources for a stream processing job.
 * An instance of KStreamContext is created for each partition group.
 */
public interface KStreamContext {

<<<<<<< HEAD
  /**
   * Returns the partition group id
   * @return partition group id
   */
  int id();

  /**
   * Returns the key serializer
   * @return the key serializer
   */
  Serializer<?> keySerializer();

  /**
   * Returns the value serializer
   * @return the value serializer
   */
  Serializer<?> valueSerializer();

  /**
   * Returns the key deserializer
   * @return the key deserializer
   */
  Deserializer<?> keyDeserializer();

  /**
   * Returns the value deserializer
   * @return the value deserializer
   */
  Deserializer<?> valueDeserializer();

  /**
   * Returns a RecordCollector
   * @return RecordCollector
   */
  RecordCollector recordCollector();

  /**
   * Returns an application context registered to {@link StreamingConfig}.
   * @return an application context
   */
  Map<String, Object> getContext();

  /**
   * Returns the state directory for the partition.
   * @return the state directory
   */
  File stateDir();

  /**
   * Returns Metrics instance
   * @return Metrics
   */
  Metrics metrics();

  /**
   * Registers and possibly restores the specified storage engine.
   * @param store the storage engine
   */
<<<<<<< HEAD
  void restore(StateStore store, RestoreFunc restoreFunc);

  /**
<<<<<<< HEAD
   * Registers the specified storage enging.
   * @param store the storage engine
   */
  void register(StateStore store);
=======
  void register(StateStore store, RestoreFunc restoreFunc);
>>>>>>> remove restore function

  /**
   * Ensures that the context is in the initialization phase where KStream topology can be constructed
<<<<<<< HEAD
=======
   * Flush the local state of this context
>>>>>>> new api model
=======
   *
   * Flush the local state of this context
>>>>>>> new api model
   */
  void flush();


  void send(String topic, Object key, Object value);

  void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

<<<<<<< HEAD
<<<<<<< HEAD
  void schedule(Processor processor, long interval);
=======
  PunctuationScheduler getPunctuationScheduler(Processor processor);
>>>>>>> new api model
=======
  void schedule(Processor processor, long interval);
>>>>>>> removed ProcessorContext

  void commit();

  String topic();

  int partition();

  long offset();

  long timestamp();
=======
    /**
     * Returns the partition group id
     *
     * @return partition group id
     */
    int id();

    /**
     * Returns the key serializer
     *
     * @return the key serializer
     */
    Serializer<?> keySerializer();

    /**
     * Returns the value serializer
     *
     * @return the value serializer
     */
    Serializer<?> valueSerializer();

    /**
     * Returns the key deserializer
     *
     * @return the key deserializer
     */
    Deserializer<?> keyDeserializer();

    /**
     * Returns the value deserializer
     *
     * @return the value deserializer
     */
    Deserializer<?> valueDeserializer();

    /**
     * Returns a RecordCollector
     *
     * @return RecordCollector
     */
    RecordCollector recordCollector();

    /**
     * Returns an application context registered to {@link StreamingConfig}.
     *
     * @return an application context
     */
    Map<String, Object> getContext();

    /**
     * Returns the state directory for the partition.
     *
     * @return the state directory
     */
    File stateDir();

    /**
     * Returns Metrics instance
     *
     * @return Metrics
     */
    Metrics metrics();

    /**
     * Registers and possibly restores the specified storage engine.
     *
     * @param store the storage engine
     */
    void register(StateStore store, RestoreFunc restoreFunc);

    /**
     * Flush the local state of this context
     */
    void flush();

    void send(String topic, Object key, Object value);

    void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

    void schedule(Processor processor, long interval);

    void commit();

    String topic();

    int partition();

    long offset();

    long timestamp();
>>>>>>> added missing files

}
