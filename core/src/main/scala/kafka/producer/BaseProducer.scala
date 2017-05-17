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

package kafka.producer

import java.util.Properties

// A base producer used whenever we need to have options for both old and new producers;
// this class will be removed once we fully rolled out 0.9
@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0")
trait BaseProducer {
  def send(topic: String, key: Array[Byte], value: Array[Byte])
  def close()
}

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0")
class NewShinyProducer(producerProps: Properties) extends BaseProducer {
  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
  import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback

  // decide whether to send synchronously based on producer properties
  val sync = producerProps.getProperty("producer.type", "async").equals("sync")

  val producer = new KafkaProducer[Array[Byte],Array[Byte]](producerProps)

  override def send(topic: String, key: Array[Byte], value: Array[Byte]) {
    val record = new ProducerRecord[Array[Byte],Array[Byte]](topic, key, value)
    if(sync) {
      this.producer.send(record).get()
    } else {
      this.producer.send(record,
        new ErrorLoggingCallback(topic, key, value, false))
    }
  }

  override def close() {
    this.producer.close()
  }
}

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0")
class OldProducer(producerProps: Properties) extends BaseProducer {

  // default to byte array partitioner
  if (producerProps.getProperty("partitioner.class") == null)
    producerProps.setProperty("partitioner.class", classOf[kafka.producer.ByteArrayPartitioner].getName)
  val producer = new kafka.producer.Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))

  override def send(topic: String, key: Array[Byte], value: Array[Byte]) {
    this.producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, key, value))
  }

  override def close() {
    this.producer.close()
  }
}

