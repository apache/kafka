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

package kafka.javaapi.producer

import kafka.utils.Utils
import kafka.producer.async.QueueItem
import java.util.Properties
import kafka.producer.{ProducerPool, ProducerConfig, Partitioner}
import kafka.serializer.Encoder

class Producer[K,V](config: ProducerConfig,
                    partitioner: Partitioner[K],
                    producerPool: ProducerPool[V],
                    populateProducerPool: Boolean = true) /* for testing purpose only. Applications should ideally */
                                                          /* use the other constructor*/
{

  private val underlying = new kafka.producer.Producer[K,V](config, partitioner, producerPool, populateProducerPool, null)

  /**
   * This constructor can be used when all config parameters will be specified through the
   * ProducerConfig object
   * @param config Producer Configuration object
   */
  def this(config: ProducerConfig) = this(config, Utils.getObject(config.partitionerClass),
    new ProducerPool[V](config, Utils.getObject(config.serializerClass)))

  /**
   * This constructor can be used to provide pre-instantiated objects for all config parameters
   * that would otherwise be instantiated via reflection. i.e. encoder, partitioner, event handler and
   * callback handler
   * @param config Producer Configuration object
   * @param encoder Encoder used to convert an object of type V to a kafka.message.Message
   * @param eventHandler the class that implements kafka.javaapi.producer.async.IEventHandler[T] used to
   * dispatch a batch of produce requests, using an instance of kafka.javaapi.producer.SyncProducer
   * @param cbkHandler the class that implements kafka.javaapi.producer.async.CallbackHandler[T] used to inject
   * callbacks at various stages of the kafka.javaapi.producer.AsyncProducer pipeline.
   * @param partitioner class that implements the kafka.javaapi.producer.Partitioner[K], used to supply a custom
   * partitioning strategy on the message key (of type K) that is specified through the ProducerData[K, T]
   * object in the  send API
   */
  def this(config: ProducerConfig,
           encoder: Encoder[V],
           eventHandler: kafka.javaapi.producer.async.EventHandler[V],
           cbkHandler: kafka.javaapi.producer.async.CallbackHandler[V],
           partitioner: Partitioner[K]) = {
    this(config, partitioner,
         new ProducerPool[V](config, encoder,
                             new kafka.producer.async.EventHandler[V] {
                               override def init(props: Properties) { eventHandler.init(props) }
                               override def handle(events: Seq[QueueItem[V]], producer: kafka.producer.SyncProducer,
                                                   encoder: Encoder[V]) {
                                 import collection.JavaConversions._
                                 import kafka.javaapi.Implicits._
                                 eventHandler.handle(asList(events), producer, encoder)
                               }
                               override def close { eventHandler.close }
                             },
                             new kafka.producer.async.CallbackHandler[V] {
                               import collection.JavaConversions._
                               override def init(props: Properties) { cbkHandler.init(props)}
                               override def beforeEnqueue(data: QueueItem[V] = null.asInstanceOf[QueueItem[V]]): QueueItem[V] = {
                                 cbkHandler.beforeEnqueue(data)
                               }
                               override def afterEnqueue(data: QueueItem[V] = null.asInstanceOf[QueueItem[V]], added: Boolean) {
                                 cbkHandler.afterEnqueue(data, added)
                               }
                               override def afterDequeuingExistingData(data: QueueItem[V] = null): scala.collection.mutable.Seq[QueueItem[V]] = {
                                 cbkHandler.afterDequeuingExistingData(data)
                               }
                               override def beforeSendingData(data: Seq[QueueItem[V]] = null): scala.collection.mutable.Seq[QueueItem[V]] = {
                                 asList(cbkHandler.beforeSendingData(asList(data)))
                               }
                               override def lastBatchBeforeClose: scala.collection.mutable.Seq[QueueItem[V]] = {
                                 asBuffer(cbkHandler.lastBatchBeforeClose)
                               }
                               override def close { cbkHandler.close }
                             }))
  }

  /**
   * Sends the data to a single topic, partitioned by key, using either the
   * synchronous or the asynchronous producer
   * @param producerData the producer data object that encapsulates the topic, key and message data
   */
  def send(producerData: kafka.javaapi.producer.ProducerData[K,V]) {
    import collection.JavaConversions._
    underlying.send(new kafka.producer.ProducerData[K,V](producerData.getTopic, producerData.getKey,
                                                         asBuffer(producerData.getData)))
  }

  /**
   * Use this API to send data to multiple topics
   * @param producerData list of producer data objects that encapsulate the topic, key and message data
   */
  def send(producerData: java.util.List[kafka.javaapi.producer.ProducerData[K,V]]) {
    import collection.JavaConversions._
    underlying.send(asBuffer(producerData).map(pd => new kafka.producer.ProducerData[K,V](pd.getTopic, pd.getKey,
                                                         asBuffer(pd.getData))): _*)
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close = underlying.close
}
