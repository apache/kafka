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

package kafka.integration

import kafka.consumer.SimpleConsumer
import org.scalatest.junit.JUnit3Suite
import java.util.Properties
import kafka.producer.{ProducerConfig, Producer}
import kafka.utils.TestUtils
import kafka.serializer._

trait ProducerConsumerTestHarness extends JUnit3Suite with KafkaServerTestHarness {
    val port: Int
    val host = "localhost"
    var producer: Producer[String, String] = null
    var consumer: SimpleConsumer = null

  override def setUp() {
      super.setUp
      val props = new Properties()
      props.put("partitioner.class", "kafka.utils.StaticPartitioner")
      props.put("broker.list", TestUtils.getBrokerListStrFromConfigs(configs))
      props.put("send.buffer.bytes", "65536")
      props.put("connect.timeout.ms", "100000")
      props.put("reconnect.interval", "10000")
      props.put("retry.backoff.ms", "1000")
      props.put("message.send.max.retries", "3")
      props.put("request.required.acks", "-1")
      props.put("serializer.class", classOf[StringEncoder].getName.toString)
      producer = new Producer(new ProducerConfig(props))
      consumer = new SimpleConsumer(host, port, 1000000, 64*1024, "")
    }

   override def tearDown() {
     producer.close()
     consumer.close()
     super.tearDown
   }
}
