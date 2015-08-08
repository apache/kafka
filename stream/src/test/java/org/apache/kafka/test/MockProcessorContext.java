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

package org.apache.kafka.test;

<<<<<<< HEAD:stream/src/test/java/org/apache/kafka/test/MockKStreamContext.java
<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Processor;
<<<<<<< HEAD
<<<<<<< HEAD
=======
import io.confluent.streaming.PunctuationScheduler;
>>>>>>> new api model
=======
>>>>>>> removed ProcessorContext
import io.confluent.streaming.RecordCollector;
<<<<<<< HEAD
import io.confluent.streaming.StorageEngine;
=======
import io.confluent.streaming.StateStore;
<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.Coordinator;
>>>>>>> wip
import io.confluent.streaming.internal.StreamGroup;
=======
import io.confluent.streaming.internal.PunctuationQueue;
=======
import io.confluent.streaming.internal.PunctuationQueue;
<<<<<<< HEAD
import io.confluent.streaming.internal.PunctuationSchedulerImpl;
>>>>>>> new api model
=======
>>>>>>> removed ProcessorContext
import io.confluent.streaming.kv.internals.RestoreFunc;
>>>>>>> new api model
=======
import org.apache.kafka.stream.topology.Processor;
=======
import org.apache.kafka.stream.Processor;
>>>>>>> wip
import org.apache.kafka.stream.KStreamContext;
<<<<<<< HEAD
import org.apache.kafka.stream.RecordCollector;
import org.apache.kafka.stream.RestoreFunc;
import org.apache.kafka.stream.StateStore;
>>>>>>> removing io.confluent imports: wip
=======
=======
import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.ProcessorContext;
>>>>>>> wip:stream/src/test/java/org/apache/kafka/test/MockProcessorContext.java
import org.apache.kafka.clients.processor.RecordCollector;
import org.apache.kafka.clients.processor.RestoreFunc;
import org.apache.kafka.clients.processor.StateStore;
>>>>>>> wip
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;

public class MockProcessorContext implements ProcessorContext {

<<<<<<< HEAD
  Serializer serializer;
  Deserializer deserializer;
  private final PunctuationQueue punctuationQueue = new PunctuationQueue();

  public MockKStreamContext(Serializer<?> serializer, Deserializer<?> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public int id() { return -1; }

  @Override
  public Serializer<?> keySerializer() { return serializer; }

  @Override
  public Serializer<?> valueSerializer() { return serializer; }

  @Override
  public Deserializer<?> keyDeserializer() { return deserializer; }

  @Override
  public Deserializer<?> valueDeserializer() { return deserializer; }

  @Override
  public RecordCollector recordCollector() { throw new UnsupportedOperationException("recordCollector() not supported."); }

  @Override
  public Map<String, Object> getContext() { throw new UnsupportedOperationException("getContext() not supported."); }

  @Override
  public File stateDir() { throw new UnsupportedOperationException("stateDir() not supported."); }

  @Override
  public Metrics metrics() { throw new UnsupportedOperationException("metrics() not supported."); }

  @Override
<<<<<<< HEAD
  public void restore(StateStore store, RestoreFunc func) { throw new UnsupportedOperationException("restore() not supported."); }
<<<<<<< HEAD

  public void register(StateStore store) { throw new UnsupportedOperationException("restore() not supported."); }
=======
>>>>>>> new api model

  public void register(StateStore store) { throw new UnsupportedOperationException("restore() not supported."); }
=======
  public void register(StateStore store, RestoreFunc func) { throw new UnsupportedOperationException("restore() not supported."); }
>>>>>>> removing io.confluent imports: wip

  @Override
  public void flush() { throw new UnsupportedOperationException("flush() not supported."); }

  @Override
<<<<<<< HEAD
<<<<<<< HEAD
  public void restore(StateStore engine) throws Exception { throw new UnsupportedOperationException("restore() not supported."); }
=======
  public void send(String topic, Object key, Object value) { throw new UnsupportedOperationException("send() not supported."); }
>>>>>>> new api model
=======
  public void send(String topic, Object key, Object value) { throw new UnsupportedOperationException("send() not supported."); }
>>>>>>> new api model

  @Override
  public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) { throw new UnsupportedOperationException("send() not supported."); }

  @Override
<<<<<<< HEAD
<<<<<<< HEAD
  public void schedule(Processor processor, long interval) {
    throw new UnsupportedOperationException("schedule() not supported");
=======
  public PunctuationScheduler getPunctuationScheduler(Processor processor) {
    return new PunctuationSchedulerImpl(punctuationQueue, processor);
>>>>>>> new api model
=======
  public void schedule(Processor processor, long interval) {
    throw new UnsupportedOperationException("schedule() not supported");
>>>>>>> removed ProcessorContext
  }

  @Override
  public void commit() { throw new UnsupportedOperationException("commit() not supported."); }

  @Override
  public String topic() { throw new UnsupportedOperationException("topic() not supported."); }

  @Override
  public int partition() { throw new UnsupportedOperationException("partition() not supported."); }

  @Override
  public long offset() { throw new UnsupportedOperationException("offset() not supported."); }

  @Override
  public long timestamp() { throw new UnsupportedOperationException("timestamp() not supported."); }
=======
    Serializer serializer;
    Deserializer deserializer;

    long timestamp = -1L;

    public MockProcessorContext(Serializer<?> serializer, Deserializer<?> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int id() {
        return -1;
    }

    @Override
    public Serializer<?> keySerializer() {
        return serializer;
    }

    @Override
    public Serializer<?> valueSerializer() {
        return serializer;
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return deserializer;
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return deserializer;
    }

    @Override
    public RecordCollector recordCollector() {
        throw new UnsupportedOperationException("recordCollector() not supported.");
    }

    @Override
    public File stateDir() {
        throw new UnsupportedOperationException("stateDir() not supported.");
    }

    @Override
    public Metrics metrics() {
        throw new UnsupportedOperationException("metrics() not supported.");
    }

    @Override
    public void register(StateStore store, RestoreFunc func) {
        throw new UnsupportedOperationException("restore() not supported.");
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("flush() not supported.");
    }

    @Override
    public void send(String topic, Object key, Object value) {
        throw new UnsupportedOperationException("send() not supported.");
    }

    @Override
    public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
        throw new UnsupportedOperationException("send() not supported.");
    }

    @Override
    public void schedule(KafkaProcessor processor, long interval) {
        throw new UnsupportedOperationException("schedule() not supported");
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException("topic() not supported.");
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException("partition() not supported.");
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException("offset() not supported.");
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }
>>>>>>> compile and test passed

}
