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

package kafka.javaapi.integration

import org.scalatest.junit.JUnit3Suite
import java.util.Properties
import kafka.producer.SyncProducerConfig
import kafka.javaapi.producer.SyncProducer
import kafka.javaapi.consumer.SimpleConsumer

trait ProducerConsumerTestHarness extends JUnit3Suite {
  
    val port: Int
    val host = "localhost"
    var producer: SyncProducer = null
    var consumer: SimpleConsumer = null

    override def setUp() {
      val props = new Properties()
      props.put("host", host)
      props.put("port", port.toString)
      props.put("buffer.size", "65536")
      props.put("connect.timeout.ms", "100000")
      props.put("reconnect.interval", "10000")
      producer = new SyncProducer(new SyncProducerConfig(props))
      consumer = new SimpleConsumer(host,
                                   port,
                                   1000000,
                                   64*1024)
      super.setUp
    }

   override def tearDown() {
     super.tearDown
     producer.close()
     consumer.close()
   }
}
