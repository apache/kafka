/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.integration

import kafka.consumer.SimpleConsumer
import org.junit.{After, Before}
import kafka.producer.Producer
import kafka.utils.{StaticPartitioner, TestUtils}
import kafka.serializer.StringEncoder

@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
trait ProducerConsumerTestHarness extends KafkaServerTestHarness {
  val host = "localhost"
  var producer: Producer[String, String] = null
  var consumer: SimpleConsumer = null

  @Before
  override def setUp() {
    super.setUp
    producer = TestUtils.createProducer[String, String](TestUtils.getBrokerListStrFromServers(servers),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName)
    consumer = new SimpleConsumer(host, servers.head.boundPort(), 1000000, 64 * 1024, "")
  }

  @After
  override def tearDown() {
    producer.close()
    consumer.close()
    super.tearDown
  }
}
