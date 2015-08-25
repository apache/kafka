<<<<<<< HEAD
package org.apache.kafka.clients.processor;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K, V>  {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  void init(KStreamContext context);
=======
  public interface ProcessorContext {

    void send(String topic, Object key, Object value);

    void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

    void schedule(long timestamp);

    void commit();

    String topic();

    int partition();

    long offset();

    long timestamp();

    KStreamContext kstreamContext();

  }

  void init(ProcessorContext context);
>>>>>>> new api model
=======
  void init(KStreamContext context);
>>>>>>> removed ProcessorContext
=======
  void init(ProcessorContext context);
>>>>>>> removing io.confluent imports: wip

  void process(K key, V value);

  void punctuate(long streamTime);

  void close();
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

package org.apache.kafka.streaming.processor;

public interface Processor<K, V> {

    void init(ProcessorContext context);

    void process(K key, V value);

    void punctuate(long streamTime);

    void close();
>>>>>>> wip
}
