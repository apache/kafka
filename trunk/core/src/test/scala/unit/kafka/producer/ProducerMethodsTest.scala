/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package unit.kafka.producer

import collection.immutable.SortedSet
import java.util._
import junit.framework.Assert._
import kafka.cluster.Partition
import kafka.common.NoBrokersForPartitionException
import kafka.producer._
import org.easymock.EasyMock
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import scala.collection.immutable.List

class ProducerMethodsTest extends JUnitSuite {

  @Test
  def producerThrowsNoBrokersException() = {
    val props = new Properties
    props.put("broker.list", "placeholder") // Need to fake out having specified one
    val config = new ProducerConfig(props)
    val mockPartitioner = EasyMock.createMock(classOf[Partitioner[String]])
    val mockProducerPool = EasyMock.createMock(classOf[ProducerPool[String]])
    val mockBrokerPartitionInfo = EasyMock.createMock(classOf[kafka.producer.BrokerPartitionInfo])

    EasyMock.expect(mockBrokerPartitionInfo.getBrokerPartitionInfo("the_topic")).andReturn(SortedSet[Partition]())
    EasyMock.replay(mockBrokerPartitionInfo)

    val producer = new Producer[String, String](config,mockPartitioner, mockProducerPool,false, mockBrokerPartitionInfo)

    try {
      val producerData = new ProducerData[String, String]("the_topic", "the_key", List("the_datum"))
      producer.send(producerData)
      fail("Should have thrown a NoBrokersForPartitionException.")
    } catch {
      case nb: NoBrokersForPartitionException => assertTrue(nb.getMessage.contains("the_key"))
    }

  }
}
